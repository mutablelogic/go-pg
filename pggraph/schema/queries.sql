-- pggraph.get
SELECT
	"name",
	"version",
	"description",
	"created_at"
FROM
	${"schema"}."graph"
WHERE
	"name" = @id
AND
	"version" = @version;

-- pggraph.upsert
INSERT INTO ${"schema"}."graph" (
	"name",
	"version",
	"description"
) VALUES (
	@id,
	@version,
	@description
)
ON CONFLICT ("name", "version")
DO UPDATE SET
	"description" = EXCLUDED."description"
RETURNING
	"name",
	"version",
	"description",
	"created_at";

-- pggraph.node_get
SELECT
	"name",
	"version",
	"description",
	"retain_count"
FROM
	${"schema"}."node"
WHERE
	"name" = @name
AND
	"version" = @version;

-- pggraph.node_upsert
INSERT INTO ${"schema"}."node" (
	"name",
	"version",
	"description",
	"in",
	"out"
) VALUES (
	@name,
	@version,
	@description,
	ROW(@in)::${"schema"}."edge",
	ARRAY(
		SELECT ROW(v)::${"schema"}."edge"
		FROM UNNEST(CAST(@out AS TEXT[])) AS v
	)
)
ON CONFLICT ("name", "version")
DO UPDATE SET
	"description" = EXCLUDED."description",
	"in" = EXCLUDED."in",
	"out" = EXCLUDED."out"
RETURNING
	"name",
	"version",
	"description",
	"retain_count";
