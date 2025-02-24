<?php declare(strict_types=1);

/**
 * DataTables PHP libraries.
 *
 * PHP libraries for DataTables and DataTables Editor.
 *
 * @author    SpryMedia
 * @copyright 2012 SpryMedia ( http://sprymedia.co.uk )
 * @license   http://editor.datatables.net/license DataTables Editor
 *
 * @see       http://editor.datatables.net
 */

namespace DataTables;

use DataTables\Database\Query;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\Query\QueryBuilder;
use Doctrine\DBAL\Result;

/**
 * DataTables Database connection object.
 *
 * Create a database connection which may then have queries performed upon it.
 *
 * This is a database abstraction class that can be used on multiple different
 * databases. As a result of this, it might not be suitable to perform complex
 * queries through this interface or vendor specific queries, but everything
 * required for basic database interaction is provided through the abstracted
 * methods.
 */
class Database
{
	/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
	 * Constructor
	 */

	/**
	 * Database instance constructor.
	 *
	 * @param array<string, string|\PDO> $opts Array of connection parameters for the database:
	 *                                         ```php
	 *                                         [
	 *                                         "user" => "", // User name
	 *                                         "pass" => "", // Password
	 *                                         "host" => "", // Host name
	 *                                         "port" => "", // Port
	 *                                         "db"   => "", // Database name
	 *                                         "type" => ""  // Datable type: "Mysql", "Postgres" or "Sqlite"
	 *                                         ]
	 *                                         ```
	 */
	public function __construct(array $opts)
	{
		$types = ['Mysql', 'Oracle', 'Postgres', 'Sqlite', 'Sqlserver', 'Db2', 'Firebird'];

		if (!in_array($opts['type'], $types)) {
			throw new \Exception(
				'Unknown database driver type. Must be one of ' . implode(', ', $types),
				1
			);
		}

		$this->_type = $opts['type'];
		$this->query_driver = 'DataTables\\Database\\Driver\\' . $opts['type'] . 'Query';
		$this->_dbResource = isset($opts['pdo']) ?
			$opts['pdo'] :
			call_user_func($this->query_driver . '::connect', $opts);

		$this->connection = DriverManager::getConnection([
			'dbname' => $opts['db'],
			'user' => $opts['user'],
			'password' => $opts['pass'],
			'host' => $opts['host'],
			'driver' => sprintf("pdo_%s", strtolower($opts['type']))
		]);
	}

	private readonly Connection $connection;

	/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
	 * Private properties
	 */

	/** @var \PDO */
	private $_dbResource;

	/** @var callable */
	private $_type;

	/** @var callable|null */
	private $_debugCallback;

	private $query_driver;

	/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
	 * Public methods
	 */

	/**
	 * Determine if there is any data in the table that matches the query
	 * condition.
	 *
	 * @param string		  $table Table name(s) to act upon.
	 * @param array           $where Where condition for what to select.
	 *
	 * @return bool Boolean flag - true if there were rows
	 */
	public function any(string $table, array $where = []): bool
	{
		$qb = $this->connection->createQueryBuilder();
		$qb->select('*')
			->from($table)
			->setMaxResults(1);

		foreach ($where as $column => $value) {
			$qb->andWhere($qb->expr()->eq($column, ":$column"))
				->setParameter($column, $value);
		}

		return (bool) $qb->executeQuery()->fetchOne();
	}

	/**
	 * Commit a database transaction.
	 *
	 * Use with {@see Database->transaction()} and {@see Database->rollback()}.
	 *
	 * @return $this
	 */
	public function commit(): static
	{
		$this->connection->commit();

		return $this;
	}

	/**
	 * Get a count from a table.
	 *
	 * @param string|string[] $table Table name(s) to act upon.
	 * @param string          $field Primary key field name
	 * @param array           $where Where condition for what to select - see {@see
	 *                               Query->where()}.
	 *
	 * @return int
	 */
	public function count($table, $field = 'id', $where = null)
	{
		$res = $this->query('count')
			->table($table)
			->get($field)
			->where($where)
			->exec();

		$cnt = $res->fetch();

		return $cnt['cnt'];
	}

	/**
	 * Get / set debug mode.
	 *
	 * @param false|callable $set Debug mode state. If not given, then used as a getter.
	 *
	 * @return ($set is null ? bool : $this) Debug mode state if no parameter is given.
	 */
	public function debug($set = null)
	{
		if ($set === null) {
			return is_callable($this->_debugCallback) ? true : false;
		} elseif ($set === false) {
			$this->_debugCallback = null;
		} else {
			$this->_debugCallback = $set;
		}

		return $this;
	}

	/**
	 * Perform a delete query on a table.
	 *
	 * @param string $table Table name to act upon.
	 * @param array  $where Where condition for what to delete.
	 *
	 * @return int Number of affected rows.
	 */
	public function delete(string $table, array $where = []): int|string
	{
		$qb = $this->connection->createQueryBuilder();
		$qb->delete($table);

		foreach ($where as $column => $value) {
			$qb->andWhere($qb->expr()->eq($column, ':' . $column))
				->setParameter($column, $value);
		}

		return $qb->executeStatement();
	}

	/**
	 * Insert data into a table.
	 *
	 * @param string $table Table name to act upon.
	 * @param array  $set   Field names and values to set.
	 * @param string $pkey  Primary key column name (optional).
	 *
	 * @return int Last inserted ID if a primary key is provided, otherwise affected rows count.
	 */
	public function insert($table, $set, $pkey = '')
	{
		$qb = $this->connection->createQueryBuilder();
		$qb->insert($table);

		foreach ($set as $column => $value) {
			$qb->setValue($column, ":set_$column")
				->setParameter("set_$column", $value);
		}

		$qb->executeStatement();

		return $pkey ? $this->connection->lastInsertId() : $qb->executeStatement();
	}

	/**
	 * Update or Insert data.
	 *
	 * @param string $table Table name to act upon.
	 * @param array  $set   Field names and values to set.
	 * @param array  $where Where condition for what to update.
	 * @param string $pkey  Primary key column name (optional).
	 *
	 * @return int Affected rows count.
	 */
	public function push($table, $set, $where = [], $pkey = '')
	{
		$qb = $this->connection->createQueryBuilder();
		$qb->select('*')->from($table);

		foreach ($where as $column => $value) {
			$qb->andWhere($qb->expr()->eq($column, ':where_' . $column))
				->setParameter('where_' . $column, $value);
		}

		if (!empty($qb->executeQuery()->fetchAssociative())) {
			return $this->update($table, $set, $where);
		}

		foreach ($where as $key => $value) {
			if (!isset($set[$key])) {
				$set[$key] = $value;
			}
		}

		return $this->insert($table, $set, $pkey);
	}

	/**
	 * Create a query object to build a database query.
	 *
	 * @param string          $type  Query type - select, insert, update or delete.
	 * @param string|string[] $table Table name(s) to act upon.
	 *
	 * @return Query
	 */
	public function query($type, $table = null): Query
	{
		return new $this->query_driver($this, $type, $table);
	}

	/**
	 * Create a QueryBuilder object for a raw SQL query.
	 * You must call `executeQuery()` or `executeStatement()` manually.
	 *
	 * @return QueryBuilder
	 *
	 * @example
	 *    Safely escape user input:
	 *    ```php
	 *    $db->raw()
	 *       ->setParameter(':date', $_POST['date'])
	 *       ->executeQuery('SELECT * FROM staff WHERE date < :date');
	 *    ```
	 */
	public function raw(): QueryBuilder
	{
		return $this->connection->createQueryBuilder();
	}

	/**
	 * Get the database resource connector. This is typically a PDO object.
	 *
	 * @return \PDO PDO connection resource (driver dependent)
	 */
	public function resource()
	{
		return $this->_dbResource;
	}

	/**
	 * Rollback the database state to the start of the transaction.
	 *
	 * Use with {@see Database->transaction()} and {@see Database->commit()}.
	 *
	 * @return $this
	 */
	public function rollback(): static
	{
		$this->connection->rollBack();

		return $this;
	}

	/**
	 * Select data from a table.
	 *
	 * @param string       $table   Table name to act upon.
	 * @param string|array $field   Fields to get from the table.
	 * @param array        $where   Where condition for what to select.
	 * @param array        $orderBy Order condition.
	 *
	 * @return array The selected rows.
	 */
	public function select($table, $field = '*', $where = [], $orderBy = [])
	{
		$qb = $this->connection->createQueryBuilder();
		$qb->select(is_array($field) ? implode(',', $field) : $field)
			->from($table);

		foreach ($where as $column => $value) {
			$qb->andWhere($qb->expr()->eq($column, ':where_' . $column))
				->setParameter('where_' . $column, $value);
		}

		foreach ($orderBy as $column => $direction) {
			$qb->addOrderBy($column, $direction);
		}

		return $qb->executeQuery()->fetchAllAssociative();
	}

	/**
	 * Select distinct data from a table.
	 *
	 * @param string       $table   Table name to act upon.
	 * @param string|array $field   Fields to get from the table.
	 * @param array        $where   Where condition for what to select.
	 * @param array        $orderBy Order condition.
	 *
	 * @return array The selected rows.
	 */
	public function selectDistinct(string $table, string|array $field = '*', array $where = [], array $orderBy = []): array
	{
		$qb = $this->connection->createQueryBuilder();
		$qb->select(is_array($field) ? implode(',', $field) : $field)
			->distinct(true)
			->from($table);

		foreach ($where as $column => $value) {
			$qb->andWhere($qb->expr()->eq($column, ":$column"))
				->setParameter($column, $value);
		}

		foreach ($orderBy as $column => $direction) {
			$qb->addOrderBy($column, $direction);
		}

		return $qb->executeQuery()->fetchAllAssociative();
	}

	/**
	 * Execute a raw SQL query.
	 *
	 * @param string $sql SQL string to execute.
	 *
	 * @return Result The query result.
	 */
	public function sql(string $sql): Result
	{
		return $this->connection->executeQuery($sql);
	}

	/**
	 * Start a new database transaction.
	 *
	 * Use with {@see Database->commit()} and {@see Database->rollback()}.
	 *
	 * @return $this
	 */
	public function transaction(): static
	{
		$this->connection->beginTransaction();

		return $this;
	}

	/**
	 * Get the database type (e.g. Postgres, Mysql, etc).
	 */
	public function type()
	{
		return $this->_type;
	}

	/**
	 * Update data in a table.
	 *
	 * @param string $table Table name to act upon.
	 * @param array  $set   Field names and values to update.
	 * @param array  $where Where condition for what to update.
	 *
	 * @return int Number of affected rows.
	 */
	public function update($table, $set = [], $where = []): int|string
	{
		$qb = $this->connection->createQueryBuilder();
		$qb->update($table);

		foreach ($set as $column => $value) {
			$qb->set($column, ":set_$column")
				->setParameter("set_$column", $value);
		}

		foreach ($where as $column => $value) {
			$qb->andWhere($qb->expr()->eq($column, ":where_$column"))
				->setParameter("where_$column", $value);
		}

		return $qb->executeStatement();
	}

	/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
	 * Internal functions
	 */

	/**
	 * Get debug query information.
	 *
	 * @return $this Information about the queries used. When this method is
	 *               called it will reset the query cache.
	 *
	 * @internal
	 */
	public function debugInfo($query = null, $bindings = null)
	{
		$callback = $this->_debugCallback;

		if ($callback) {
			$callback([
				'query' => $query,
				'bindings' => $bindings,
			]);
		}

		return $this;
	}
}
