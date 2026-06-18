// KVMQL Driver — Driver trait and implementations (Phase 2)

pub mod aws;
pub mod azure;
pub mod capability;
pub mod cloudflare;
pub mod firecracker;
pub mod gcp;
pub mod github;
pub mod k8s;
pub mod mock;
pub mod provision;
pub mod simulate;
pub mod ssh;
pub mod traits;
pub mod types;

#[cfg(test)]
mod cloud_driver_tests {
    use crate::aws::AwsEc2Driver;
    use crate::azure::AzureVmDriver;
    use crate::capability::Capability;
    use crate::firecracker::FirecrackerDriver;
    use crate::gcp::GcpComputeDriver;
    use crate::traits::Driver;

    #[test]
    fn test_all_drivers_have_create() {
        let aws = AwsEc2Driver::new("us-east-1");
        let gcp = GcpComputeDriver::new("my-project");
        let azure = AzureVmDriver::new("sub-12345");

        assert!(aws.capabilities().supports(&Capability::Create));
        assert!(gcp.capabilities().supports(&Capability::Create));
        assert!(azure.capabilities().supports(&Capability::Create));
    }

    #[test]
    fn test_kvm_exclusive_capabilities() {
        let fc = FirecrackerDriver::new("/tmp/fc.sock");
        let aws = AwsEc2Driver::new("us-east-1");
        let gcp = GcpComputeDriver::new("my-project");
        let azure = AzureVmDriver::new("sub-12345");

        // Pause, Resume, CustomKernel, Vsock are KVM-exclusive (Firecracker only)
        let kvm_only = [
            Capability::Pause,
            Capability::Resume,
            Capability::CustomKernel,
            Capability::Vsock,
        ];

        for cap in &kvm_only {
            assert!(
                fc.capabilities().supports(cap),
                "Firecracker should support {:?}",
                cap
            );
            assert!(
                !aws.capabilities().supports(cap),
                "AWS should NOT support {:?}",
                cap
            );
            assert!(
                !gcp.capabilities().supports(cap),
                "GCP should NOT support {:?}",
                cap
            );
            assert!(
                !azure.capabilities().supports(cap),
                "Azure should NOT support {:?}",
                cap
            );
        }
    }
}
