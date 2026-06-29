use kvmql_registry::Registry;

/// A fetched pricing entry ready for DB insert.
struct PricingEntry {
    provider: String,
    region: String,
    resource_type: String,
    param: String,
    hourly: f64,
    monthly: f64,
    unit: String,
}

/// Fetch live Azure pricing from the public Retail Prices API and upsert into
/// the registry's pricing table. Returns the number of entries upserted.
pub fn update_azure_pricing(registry: &Registry, regions: &[&str]) -> Result<usize, String> {
    // Fetch in a separate thread to avoid reqwest::blocking panic inside tokio runtime
    let regions_owned: Vec<String> = regions.iter().map(|s| s.to_string()).collect();
    let entries = std::thread::spawn(move || fetch_azure_entries(&regions_owned))
        .join()
        .map_err(|_| "Azure pricing fetch thread panicked".to_string())??;

    let mut count = 0;
    for e in &entries {
        let _ = registry.insert_pricing(
            &e.provider,
            &e.region,
            &e.resource_type,
            &e.param,
            e.hourly,
            e.monthly,
            &e.unit,
        );
        count += 1;
    }
    Ok(count)
}

fn fetch_azure_entries(regions: &[String]) -> Result<Vec<PricingEntry>, String> {
    let client = reqwest::blocking::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .map_err(|e| format!("HTTP client error: {e}"))?;

    let mut entries = Vec::new();

    let service_map: &[(&str, &str)] = &[
        ("Azure Database for PostgreSQL", "postgres"),
        ("Azure Cache for Redis", "redis"),
        ("Container Registry", "container_registry"),
        ("Load Balancer", "load_balancer"),
        ("Azure DNS", "dns_zone"),
        ("Virtual Machines", "aks"),
    ];

    for region in regions {
        for (service_name, our_type) in service_map {
            let url = format!(
                "https://prices.azure.com/api/retail/prices?\
                 $filter=serviceName eq '{}' and armRegionName eq '{}' \
                 and priceType eq 'Consumption'&$top=200",
                service_name, region
            );

            let resp = match client.get(&url).send() {
                Ok(r) => r,
                Err(e) => {
                    eprintln!("  Warning: failed to fetch {service_name} for {region}: {e}");
                    continue;
                }
            };

            let data: serde_json::Value = match resp.json() {
                Ok(d) => d,
                Err(e) => {
                    eprintln!("  Warning: failed to parse response for {service_name}: {e}");
                    continue;
                }
            };

            if let Some(items) = data["Items"].as_array() {
                for item in items {
                    let sku = item["skuName"].as_str().unwrap_or("");
                    let meter = item["meterName"].as_str().unwrap_or("");
                    let unit_price = item["retailPrice"].as_f64().unwrap_or(0.0);
                    let unit = item["unitOfMeasure"].as_str().unwrap_or("");

                    if unit_price <= 0.0 {
                        continue;
                    }
                    if meter.contains("Reserved")
                        || meter.contains("Spot")
                        || meter.contains("Low Priority")
                    {
                        continue;
                    }

                    let (hourly, monthly) = if unit.contains("Hour") {
                        (unit_price, (unit_price * 730.0 * 100.0).round() / 100.0)
                    } else if unit.contains("Month") {
                        (
                            (unit_price / 730.0 * 100_000.0).round() / 100_000.0,
                            unit_price,
                        )
                    } else {
                        continue;
                    };

                    entries.push(PricingEntry {
                        provider: "azure".into(),
                        region: region.clone(),
                        resource_type: our_type.to_string(),
                        param: if sku.is_empty() {
                            meter.to_string()
                        } else {
                            sku.to_string()
                        },
                        hourly,
                        monthly,
                        unit: "instance".into(),
                    });
                }
            }
        }
    }

    Ok(entries)
}

/// Fetch live AWS pricing via the `aws` CLI (best-effort; requires credentials).
/// Returns the number of entries upserted.
pub fn update_aws_pricing(registry: &Registry, regions: &[&str]) -> Result<usize, String> {
    use std::process::Command;

    // Verify the aws CLI is available.
    let check = Command::new("aws").arg("--version").output();
    match check {
        Ok(ref o) if o.status.success() => {}
        _ => return Err("aws CLI not available — using embedded pricing data".into()),
    }

    let mut count = 0;

    let instance_types = [
        "t3.micro",
        "t3.small",
        "t3.medium",
        "t3.large",
        "t3.xlarge",
        "m5.large",
        "m5.xlarge",
        "m5.2xlarge",
        "c6i.large",
        "c6i.xlarge",
        "r6i.large",
        "r6i.xlarge",
        "g5.xlarge",
        "g5.2xlarge",
    ];

    for region in regions {
        let location = match *region {
            "us-east-1" => "US East (N. Virginia)",
            "us-west-2" => "US West (Oregon)",
            "eu-west-1" => "EU (Ireland)",
            "ap-southeast-1" => "Asia Pacific (Singapore)",
            "ap-northeast-1" => "Asia Pacific (Tokyo)",
            "af-south-1" => "Africa (Cape Town)",
            _ => continue,
        };

        for itype in &instance_types {
            let output = Command::new("aws")
                .args([
                    "pricing",
                    "get-products",
                    "--service-code",
                    "AmazonEC2",
                    "--region",
                    "us-east-1",
                    "--filters",
                    &format!("Type=TERM_MATCH,Field=instanceType,Value={itype}"),
                    &format!("Type=TERM_MATCH,Field=location,Value={location}"),
                    "Type=TERM_MATCH,Field=operatingSystem,Value=Linux",
                    "Type=TERM_MATCH,Field=tenancy,Value=Shared",
                    "Type=TERM_MATCH,Field=preInstalledSw,Value=NA",
                    "Type=TERM_MATCH,Field=capacitystatus,Value=Used",
                    "--output",
                    "json",
                    "--no-cli-pager",
                ])
                .env("AWS_PAGER", "")
                .output();

            let output = match output {
                Ok(o) if o.status.success() => o,
                _ => continue,
            };

            let json: serde_json::Value = match serde_json::from_slice(&output.stdout) {
                Ok(v) => v,
                Err(_) => continue,
            };

            if let Some(price_list) = json["PriceList"].as_array() {
                for product_val in price_list {
                    // PriceList items may be JSON strings that need re-parsing.
                    let product: serde_json::Value = if let Some(s) = product_val.as_str() {
                        serde_json::from_str(s).unwrap_or_default()
                    } else {
                        product_val.clone()
                    };

                    if let Some(terms) = product.pointer("/terms/OnDemand") {
                        if let Some(obj) = terms.as_object() {
                            for term in obj.values() {
                                if let Some(dims) = term["priceDimensions"].as_object() {
                                    for dim in dims.values() {
                                        let hourly: f64 = dim["pricePerUnit"]["USD"]
                                            .as_str()
                                            .and_then(|s| s.parse().ok())
                                            .unwrap_or(0.0);
                                        if hourly > 0.0 {
                                            let monthly = (hourly * 730.0 * 100.0).round() / 100.0;
                                            let _ = registry.insert_pricing(
                                                "aws",
                                                region,
                                                "eks_nodegroup",
                                                itype,
                                                hourly,
                                                monthly,
                                                "instance",
                                            );
                                            count += 1;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    Ok(count)
}
