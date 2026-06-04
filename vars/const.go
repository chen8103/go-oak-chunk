package vars

// query sql
const (
	TableInfoSQL = "show create table %s"

	TableExistsSQL = `
        SELECT COUNT(*) AS count
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA= ?
            AND TABLE_NAME= ?`

	LockTableSQL = "LOCK TABLES %s.%s READ"

	UnlockTableSQL = "UNLOCK TABLES"

	FirstSQL = "select /*!40001 SQL_NO_CACHE */ %s from %s where %s"

	// PartitionsSQL lists the partition names of a table in ordinal order
	// (OceanBase / MySQL information_schema). Non-partitioned tables return
	// no rows (PARTITION_NAME IS NULL).
	PartitionsSQL = `
        SELECT PARTITION_NAME
        FROM information_schema.PARTITIONS
        WHERE TABLE_SCHEMA = ?
            AND TABLE_NAME = ?
            AND PARTITION_NAME IS NOT NULL
        ORDER BY PARTITION_ORDINAL_POSITION`

	// TiDBPKTypeSQL reports whether a TiDB table is CLUSTERED or NONCLUSTERED.
	// Only NONCLUSTERED tables expose the hidden `_tidb_rowid` handle used by
	// TiDBRowIDStrategy.
	TiDBPKTypeSQL = `
        SELECT TIDB_PK_TYPE
        FROM information_schema.tables
        WHERE TABLE_SCHEMA = ?
            AND TABLE_NAME = ?`

	// TiDBRowIDProbeSQL probes whether `_tidb_rowid` is selectable, used as a
	// fallback on TiDB versions that lack the TIDB_PK_TYPE column. Takes the
	// quoted `db`.`table` reference.
	TiDBRowIDProbeSQL = "SELECT _tidb_rowid FROM %s LIMIT 1"
)

const (
	ConstraintNoConstraint int = iota
	ConstraintPrimaryKey
	ConstraintKey
	ConstraintIndex
	ConstraintUniq
	ConstraintUniqKey
	ConstraintUniqIndex
	ConstraintForeignKey
	ConstraintFulltext
	ConstraintCheck
)

const LagThreshold int64 = -1

const Billion = 1000000000
