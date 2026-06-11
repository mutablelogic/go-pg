package schema

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

const (
	DatabaseListLimit        = 50
	ConnectionListLimit      = 50
	ExtensionListLimit       = 50
	SchemaListLimit          = 20
	ObjectListLimit          = 20
	SettingListLimit         = 100
	RoleListLimit            = 50
	TablespaceListLimit      = 50
	ReplicationSlotListLimit = 50
	StatementListLimit       = 50
)

const (
	CatalogSchema  = "pg_catalog"
	DefaultAclRole = "PUBLIC"
)

const (
	defaultSchema        = "public"
	reservedPrefix       = "pg_"
	pgTimestampFormat    = "2006-01-02 15:04:05"
	pgObfuscatedPassword = "********"
)
