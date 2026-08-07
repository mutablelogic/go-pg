-- pggraph.graph
CREATE TABLE IF NOT EXISTS ${"schema"}."graph" (
	"name"        TEXT NOT NULL CHECK ("name" ~ '^[A-Za-z_][A-Za-z0-9_]*$'),
	"version"     TEXT NOT NULL CHECK ("version" ~ '^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*$'),
	"description" TEXT,
	"created_at"  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
	PRIMARY KEY ("name", "version")
);

-- pggraph.edge_type
DO $$ BEGIN
	CREATE TYPE ${"schema"}."edge" AS (
		"type" TEXT
	);
EXCEPTION
	WHEN duplicate_object THEN null;
END $$;

-- pggraph.node
CREATE TABLE IF NOT EXISTS ${"schema"}."node" (
	"id"           BIGINT GENERATED ALWAYS AS IDENTITY UNIQUE,
	"name"         TEXT NOT NULL CHECK ("name" ~ '^[A-Za-z_][A-Za-z0-9_]*$'),
	"version"      TEXT NOT NULL CHECK ("version" ~ '^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*$'),
	"description"  TEXT,
	"in"           ${"schema"}."edge" NOT NULL,
	"out"          ${"schema"}."edge"[] NOT NULL DEFAULT ARRAY[]::${"schema"}."edge"[],
	"retain_count" BIGINT NOT NULL DEFAULT 0 CHECK ("retain_count" >= 0),
	CHECK (CARDINALITY("out") >= 1),
	PRIMARY KEY ("name", "version")
);
