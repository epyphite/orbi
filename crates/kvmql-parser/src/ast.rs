// KVMQL Abstract Syntax Tree definitions.

// ── Top Level ──────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub struct Program {
    pub statements: Vec<Statement>,
}

// ── Statement ──────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    Select(SelectStmt),
    CreateMicrovm(CreateMicrovmStmt),
    CreateVolume(CreateVolumeStmt),
    AlterMicrovm(AlterMicrovmStmt),
    AlterVolume(AlterVolumeStmt),
    Destroy(DestroyStmt),
    Pause(PauseStmt),
    Resume(ResumeStmt),
    Snapshot(SnapshotStmt),
    Restore(RestoreStmt),
    Watch(WatchStmt),
    Attach(AttachStmt),
    Detach(DetachStmt),
    Resize(ResizeStmt),
    ImportImage(ImportImageStmt),
    PublishImage(PublishImageStmt),
    RemoveImage(RemoveImageStmt),
    AddProvider(AddProviderStmt),
    RemoveProvider(RemoveProviderStmt),
    AlterProvider(AlterProviderStmt),
    AddCluster(AddClusterStmt),
    AlterCluster(AlterClusterStmt),
    RemoveCluster(RemoveClusterStmt),
    AddPrincipal(AddPrincipalStmt),
    Grant(GrantStmt),
    Revoke(RevokeStmt),
    Set(SetStmt),
    Show(ShowStmt),
    CreateResource(CreateResourceStmt),
    AlterResource(AlterResourceStmt),
    DestroyResource(DestroyResourceStmt),
    Backup(BackupStmt),
    RestoreResource(RestoreResourceStmt),
    Scale(ScaleStmt),
    Upgrade(UpgradeStmt),
    Explain(Box<Statement>),
    Rollback(RollbackStmt),
    Assert(AssertStmt),
    ImportResources(ImportResourcesStmt),
}

// ── Fields and Selection ───────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub enum FieldList {
    All,
    Fields(Vec<Field>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum Field {
    Simple(String),
    Qualified(String, String),
    /// Aggregate or scalar function applied in a SELECT projection.
    /// `count(*)` is represented as `FnCall { name: "count", star: true, args: [] }`.
    /// Other forms like `sum(price)` carry their args in `args`.
    FnCall {
        name: String,
        star: bool,
        args: Vec<Expr>,
    },
    /// `<field> AS <alias>` — wraps any other Field with an alias name.
    Aliased {
        field: Box<Field>,
        alias: String,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderItem {
    pub field: String,
    pub direction: SortDirection,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SortDirection {
    Asc,
    Desc,
}

impl Default for SortDirection {
    fn default() -> Self {
        Self::Asc
    }
}

// ── Nouns ──────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub enum Noun {
    Microvms,
    Volumes,
    Images,
    Providers,
    Clusters,
    Capabilities,
    Snapshots,
    Metrics,
    Events,
    QueryHistory,
    AuditLog,
    Principals,
    Grants,
    ClusterMembers,
    Resources,
    Plans,
    AppliedFiles,
    // ── Kubernetes live-query nouns ─────────────────────────
    // These don't hit the local registry — they shell out to `kubectl get`
    // against the cluster the bound provider points at.
    K8sPods,
    K8sDeployments,
    K8sServices,
    K8sIngresses,
    K8sConfigmaps,
    K8sSecrets,
    K8sNamespaces,
    K8sNodes,
    ImportLog,
}

// ── Target Spec ────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub struct TargetSpec {
    pub target: TargetKind,
    pub live: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TargetKind {
    Provider(String),
    Cluster(String),
}

// ── Predicates (WHERE clause) ──────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub enum Predicate {
    And(Box<Predicate>, Box<Predicate>),
    Or(Box<Predicate>, Box<Predicate>),
    Not(Box<Predicate>),
    Comparison(Comparison),
    Grouped(Box<Predicate>),
    /// `EXISTS ( SELECT ... )` — true iff the inner SELECT returns >=1 row.
    Exists(Box<SelectStmt>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct Comparison {
    pub left: Expr,
    pub op: ComparisonOp,
    pub right: Expr,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ComparisonOp {
    Eq,
    NotEq,
    Gt,
    Lt,
    GtEq,
    LtEq,
    In,
    NotIn,
    Like,
    IsNull,
    IsNotNull,
}

// ── Expressions ────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Identifier(String),
    StringLit(String),
    Integer(i64),
    Float(f64),
    Boolean(bool),
    Null,
    Duration(DurationValue),
    FunctionCall(FunctionCall),
    BinaryOp {
        left: Box<Expr>,
        op: BinaryOp,
        right: Box<Expr>,
    },
    Grouped(Box<Expr>),
    Variable(String),
    /// A scalar subquery `( SELECT ... )`. Evaluates to the first column of
    /// the first row, or NULL if the subquery returned no rows.
    Subquery(Box<SelectStmt>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum BinaryOp {
    Add,
    Subtract,
    /// SQL string concatenation (`||`).
    Concat,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DurationValue {
    pub magnitude: i64,
    pub unit: DurationUnit,
}

#[derive(Debug, Clone, PartialEq)]
pub enum DurationUnit {
    Seconds,
    Minutes,
    Hours,
    Days,
}

#[derive(Debug, Clone, PartialEq)]
pub struct FunctionCall {
    pub name: String,
    pub args: Vec<Expr>,
}

// ── Values ─────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    String(String),
    Integer(i64),
    Float(f64),
    Boolean(bool),
    Duration(DurationValue),
    Map(Vec<MapEntry>),
    Array(Vec<Value>),
    Null,
    Variable(String),
}

#[derive(Debug, Clone, PartialEq)]
pub struct MapEntry {
    pub key: String,
    pub value: Value,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Param {
    pub key: String,
    pub value: Value,
}

pub type SetItem = Param;

// ── Statement Structs ──────────────────────────────────────────────

/// A SELECT `FROM` clause can target either a built-in noun (registry/k8s
/// table) or a table-valued function call such as `dns_lookup('example.com')`.
#[derive(Debug, Clone, PartialEq)]
pub enum SelectSource {
    Noun(Noun),
    Function(FunctionCall),
}

#[derive(Debug, Clone, PartialEq)]
pub struct SelectStmt {
    pub fields: FieldList,
    pub from: SelectSource,
    pub on: Option<TargetSpec>,
    pub where_clause: Option<Predicate>,
    pub group_by: Option<FieldList>,
    pub order_by: Option<Vec<OrderItem>>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AssertStmt {
    /// The condition must hold. If it evaluates to false, the ASSERT fails
    /// and the engine returns an error (optionally carrying `message`).
    pub condition: Predicate,
    pub message: Option<String>,
}

// ── IMPORT RESOURCES ──────────────────────────────────────────────

/// `IMPORT RESOURCES FROM PROVIDER 'x' [WHERE resource_type = '...']`
/// `IMPORT RESOURCES FROM PROVIDERS WHERE type = '...'`
/// `IMPORT RESOURCES FROM ALL PROVIDERS`
#[derive(Debug, Clone, PartialEq)]
pub struct ImportResourcesStmt {
    pub source: ImportSource,
    /// Optional filter on resource_type(s) to import.
    pub resource_type_filter: Option<Vec<String>>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ImportSource {
    /// `FROM PROVIDER 'provider-id'`
    SingleProvider(String),
    /// `FROM PROVIDERS WHERE type = 'aws'`
    ProvidersByType(String),
    /// `FROM ALL PROVIDERS`
    AllProviders,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateMicrovmStmt {
    pub if_not_exists: bool,
    pub params: Vec<Param>,
    pub volumes: Vec<VolumeInline>,
    pub on: Option<TargetSpec>,
    pub placement_policy: Option<String>,
    pub require: Vec<RequireClause>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct VolumeInline {
    pub params: Vec<Param>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum RequireClause {
    Capability(String),
    Provider(String),
    Label { key: String, value: String },
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateVolumeStmt {
    pub if_not_exists: bool,
    pub params: Vec<Param>,
    pub on: Option<TargetSpec>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AlterMicrovmStmt {
    pub id: String,
    pub set_items: Vec<SetItem>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AlterVolumeStmt {
    pub id: String,
    pub set_items: Vec<SetItem>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DestroyStmt {
    pub target: DestroyTarget,
    pub id: String,
    pub force: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum DestroyTarget {
    Microvm,
    Volume,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PauseStmt {
    pub id: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ResumeStmt {
    pub id: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SnapshotStmt {
    pub id: String,
    pub destination: String,
    pub tag: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RestoreStmt {
    pub id: String,
    pub source: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WatchStmt {
    pub metrics: FieldList,
    pub from: Noun,
    pub where_clause: Option<Predicate>,
    pub interval: DurationValue,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AttachStmt {
    pub volume_id: String,
    pub microvm_id: String,
    pub device_name: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DetachStmt {
    pub volume_id: String,
    pub microvm_id: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ResizeStmt {
    pub volume_id: String,
    pub new_size_gb: i64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ImportImageStmt {
    pub params: Vec<Param>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PublishImageStmt {
    pub image_id: String,
    pub provider: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RemoveImageStmt {
    pub image_id: String,
    pub force: bool,
}

// ── Provider Management ────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub struct AddProviderStmt {
    pub if_not_exists: bool,
    pub params: Vec<Param>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RemoveProviderStmt {
    pub name: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AlterProviderStmt {
    pub name: String,
    pub set_items: Vec<SetItem>,
}

// ── Cluster Management ─────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub struct AddClusterStmt {
    pub if_not_exists: bool,
    pub name: String,
    pub members: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AlterClusterStmt {
    pub name: String,
    pub action: ClusterAlterAction,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ClusterAlterAction {
    AddMember(String),
    RemoveMember(String),
}

#[derive(Debug, Clone, PartialEq)]
pub struct RemoveClusterStmt {
    pub name: String,
}

// ── Access Control ─────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub struct AddPrincipalStmt {
    pub if_not_exists: bool,
    pub params: Vec<Param>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct GrantStmt {
    pub verbs: Vec<Verb>,
    pub scope: GrantScope,
    pub where_clause: Option<Predicate>,
    pub principal: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RevokeStmt {
    pub verbs: Vec<Verb>,
    pub scope: GrantScope,
    pub principal: String,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Verb {
    Select,
    Create,
    Alter,
    Destroy,
    Pause,
    Resume,
    Snapshot,
    Restore,
    Attach,
    Detach,
    Resize,
    Watch,
    Import,
    Publish,
}

#[derive(Debug, Clone, PartialEq)]
pub enum GrantScope {
    Cluster(String),
    Provider(String),
    Microvms,
    Volumes,
    Images,
}

// ── Resource Management ────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub struct CreateResourceStmt {
    pub if_not_exists: bool,
    pub resource_type: String,
    pub params: Vec<Param>,
    pub on: Option<TargetSpec>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AlterResourceStmt {
    pub resource_type: String,
    pub id: String,
    pub set_items: Vec<SetItem>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DestroyResourceStmt {
    pub resource_type: String,
    pub id: String,
    pub force: bool,
}

// ── Day-2 Operations ──────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub struct BackupStmt {
    pub resource_type: String,
    pub id: String,
    pub destination: Option<String>,
    pub tag: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RestoreResourceStmt {
    pub resource_type: String,
    pub id: String,
    pub source: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ScaleStmt {
    pub resource_type: String,
    pub id: String,
    pub params: Vec<Param>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct UpgradeStmt {
    pub resource_type: String,
    pub id: String,
    pub params: Vec<Param>,
}

// ── Config ─────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub struct SetStmt {
    pub key: String,
    pub value: Value,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ShowStmt {
    pub target: ShowTarget,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ShowTarget {
    Providers,
    Clusters,
    Capabilities { for_provider: Option<String> },
    Grants { for_principal: Option<String> },
    Images,
    Version,
}

// ── Rollback ──────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub struct RollbackStmt {
    pub target: RollbackTarget,
}

#[derive(Debug, Clone, PartialEq)]
pub enum RollbackTarget {
    Last,
    Tag(String),
    Resource { resource_type: String, id: String },
}
