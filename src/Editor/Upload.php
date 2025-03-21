<?php

/**
 * DataTables PHP libraries.
 *
 * PHP libraries for DataTables and DataTables Editor.
 *
 * @author    SpryMedia
 * @copyright 2015 SpryMedia ( http://sprymedia.co.uk )
 * @license   http://editor.datatables.net/license DataTables Editor
 *
 * @see       http://editor.datatables.net
 */

namespace DataTables\Editor;

use DataTables\Database;
use DataTables\Database\Query;
use DataTables\Editor;
use DataTables\Ext;

/**
 * Upload class for Editor. This class provides the ability to easily specify
 * file upload information, specifically how the file should be recorded on
 * the server (database and file system).
 *
 * An instance of this class is attached to a field using the {@see
 * Field->upload()} method. When Editor detects a file upload for that file the
 * information provided for this instance is executed.
 *
 * The configuration is primarily driven through the {@see Upload->db()} and
 * {@see Upload->action()} methods:
 *
 * * {@see Upload->db()} Describes how information about the uploaded file is to be
 *   stored on the database.
 * * {@see Upload->action()} Describes where the file should be stored on the file system
 *   and provides the option of specifying a custom action when a file is
 *   uploaded.
 *
 * Both methods are optional - you can store the file on the server using the
 * {@see Upload->db()} method only if you want to store the file in the database, or if
 * you don't want to store relational data on the database us only
 * {@see Upload->action()}. However,  the majority of the time it is best to use
 * both - store information about the file on the database for fast retrieval (using a
 * {@see Editor->leftJoin()} for example) and the file on the file system for direct
 * web access.
 *
 * @example
 * 	 Store information about a file in a table called `files` and the actual
 * 	 file in an `uploads` directory.
 *   ```
 *		new Field( 'imageId' )
 *			->upload(
 *				new Upload( $_SERVER['DOCUMENT_ROOT'].'/uploads/__ID__.__EXTN__' )
 *			 		->db( 'files', 'id', [
 *						'webPath'     => Upload::DB_WEB_PATH,
 *						'fileName'    => Upload::DB_FILE_NAME,
 *						'fileSize'    => Upload::DB_FILE_SIZE,
 *						'systemPath'  => Upload::DB_SYSTEM_PATH
 *					] )
 *					->allowedExtensions( [ 'png', 'jpg' ], "Please upload an image file" )
 *			)
 *	```
 */
class Upload extends Ext
{
	/*  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *
	 * Constants
	 */

	/** Database value option (`Db()`) - File content. This should be written to
	 * a blob. Typically this should be avoided and the file saved on the file
	 * system, but there are cases where it can be useful to store the file in
	 * the database.
	 */
	const DB_CONTENT = 'editor-content';

	/** Database value option (`Db()`) - Content type */
	const DB_CONTENT_TYPE = 'editor-contentType';

	/** Database value option (`Db()`) - File extension */
	const DB_EXTN = 'editor-extn';

	/** Database value option (`Db()`) - File name (with extension) */
	const DB_FILE_NAME = 'editor-fileName';

	/** Database value option (`Db()`) - File size (bytes) */
	const DB_FILE_SIZE = 'editor-fileSize';

	/** Database value option (`Db()`) - MIME type */
	const DB_MIME_TYPE = 'editor-mimeType';

	/** Database value option (`Db()`) - Full system path to the file */
	const DB_SYSTEM_PATH = 'editor-systemPath';

	/** Database value option (`Db()`) - HTTP path to the file. This is derived
	 * from the system path by removing `$_SERVER['DOCUMENT_ROOT']`. If your
	 * images live outside of the document root a custom value would be to be
	 * used.
	 */
	const DB_WEB_PATH = 'editor-webPath';

	/** Read from the database - don't write to it.
	 */
	const DB_READ_ONLY = 'editor-readOnly';

	/*  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *
	 * Private parameters
	 */

	private $_action;
	private $_dbCleanCallback;
	private $_dbCleanTableField;
	private $_dbTable;
	private $_dbPKey;
	private $_dbFields;
	private $_dbFormat;
	private $_extns;
	private $_extnError;
	private $_error;
	private $_mode = 0644;
	private $_validators = [];
	private $_where = [];

	/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
	 * Constructor
	 */

	/**
	 * Upload instance constructor.
	 *
	 * @param string|\Closure(array, int): int $action Action to take on upload - this is applied
	 *                                                 directly to {@see Upload->action()}.
	 */
	public function __construct($action = null)
	{
		if ($action) {
			$this->action($action);
		}
	}

	/*  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *
	 * Public methods
	 */

	/**
	 * Set the action to take when a file is uploaded. This can be either of:.
	 *
	 * * A string - the value given is the full system path to where the
	 *   uploaded file is written to. The value given can include three "macros"
	 *   which are replaced by the script dependent on the uploaded file:
	 *   * `__EXTN__` - the file extension
	 *   * `__NAME__` - the uploaded file's name (including the extension)
	 *   * `__ID__` - Database primary key value if the {@see Upload->db()} method is
	 *     used.
	 * * A closure - if a function is given the responsibility of what to do
	 *   with the uploaded file is transferred to this function. That will
	 *   typically involve writing it to the file system so it can be used
	 *   later.
	 *
	 * @param string|\Closure(array, int): int $action Action to take - see description above.
	 *
	 * @return $this
	 */
	public function action($action)
	{
		$this->_action = $action;

		return $this;
	}

	/**
	 * An array of valid file extensions that can be uploaded. This is for
	 * simple validation that the file is of the expected type - for example you
	 * might use `[ 'png', 'jpg', 'jpeg', 'gif' ]` for images. The check is
	 * case-insensitive. If no extensions are given, no validation is performed
	 * on the file extension.
	 *
	 * @param string[] $extn  List of file extensions that are allowable for
	 *                        the upload
	 * @param string   $error Error message if a file is uploaded that doesn't
	 *                        match the valid list of extensions.
	 *
	 * @return $this
	 *
	 * @deprecated Use Validate::fileExtensions
	 */
	public function allowedExtensions($extn, $error = 'This file type cannot be uploaded')
	{
		$this->_extns = $extn;
		$this->_extnError = $error;

		return $this;
	}

	/**
	 * Database configuration method. When used, this method will tell Editor
	 * what information you want written to a database on file upload, should
	 * you wish to store relational information about your file on the database
	 * (this is generally recommended).
	 *
	 * @param string                 $table  The name of the table where the file information
	 *                                       should be stored
	 * @param string                 $pkey   Primary key column name. The `Upload` class
	 *                                       requires that the database table have a single primary key so each
	 *                                       row can be uniquely identified.
	 * @param array                  $fields A list of the fields to be written to on upload.
	 *                                       The property names are the database columns and the values can be
	 *                                       defined by the constants of this class. The value can also be a
	 *                                       string or a closure function if you wish to send custom information
	 *                                       to the database.
	 * @param callable(array&): void $format Formatting function that can change the data obtained
	 *                                       from the database. Only gets a single parameter passed in - the
	 *                                       database row for the file that is read.
	 *
	 * @return $this
	 */
	public function db($table, $pkey, $fields, $format = null)
	{
		$this->_dbTable = $table;
		$this->_dbPKey = $pkey;
		$this->_dbFields = $fields;
		$this->_dbFormat = $format;

		return $this;
	}

	/**
	 * Set a callback function that is used to remove files which no longer have
	 * a reference in a source table.
	 *
	 * @param callable(array): bool $callback Function that will be executed on clean. It is
	 *                                        given an array of information from the database about the orphaned
	 *                                        rows, and can return true to indicate that the rows should be
	 *                                        removed from the database. Any other return value (including none)
	 *                                        will result in the records being retained.
	 *
	 * @return $this
	 */
	public function dbClean($tableField, $callback = null)
	{
		// Argument swapping
		if ($callback === null) {
			$callback = $tableField;
			$tableField = null;
		}

		$this->_dbCleanCallback = $callback;
		$this->_dbCleanTableField = $tableField;

		return $this;
	}

	/**
	 * Set the permissions on the file after it has been uploaded using
	 * chmod.
	 *
	 * @return $this
	 */
	public function mode($m)
	{
		$this->_mode = $m;

		return $this;
	}

	/**
	 * Add a validation method to check file uploads. Multiple validators can be
	 * added by calling this method multiple times - they will be executed in
	 * sequence when a file has been uploaded.
	 *
	 * @param callable(array): ?string $fn Validation function. A PHP `$_FILES` parameter is
	 *                                     passed in for the uploaded file and the return is either a string
	 *                                     (validation failed and error message), or `null` (validation passed).
	 *
	 * @return $this
	 */
	public function validator($fn)
	{
		$this->_validators[] = $fn;

		return $this;
	}

	/**
	 * Add a condition to the data to be retrieved from the database. This
	 * must be given as a function to be executed (usually anonymous) and
	 * will be passed in a single argument, the `Query` object, to which
	 * conditions can be added. Multiple calls to this method can be made.
	 *
	 * @param \Closure(Query): void $fn Where function.
	 *
	 * @return $this
	 */
	public function where($fn)
	{
		$this->_where[] = $fn;

		return $this;
	}

	/*  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *
	 * Internal methods
	 */

	/**
	 * Get database information data from the table.
	 *
	 * @param Database $db  Database
	 * @param number[] $ids Limit to a specific set of ids
	 *
	 * @return array|null Database information
	 *
	 * @internal
	 */
	public function data($db, $ids = null)
	{
		if (!$this->_dbTable) {
			return null;
		}

		// Select the details requested, for the columns requested
		$q = $db
			->query('select')
			->table($this->_dbTable)
			->get($this->_dbPKey);

		foreach ($this->_dbFields as $column => $prop) {
			if ($prop !== self::DB_CONTENT) {
				$q->get($column);
			}
		}

		if ($ids !== null) {
			$q->where_in($this->_dbPKey, $ids);
		}

		for ($i = 0, $ien = count($this->_where); $i < $ien; ++$i) {
			$q->where($this->_where[$i]);
		}

		$result = $q->exec()->fetchAll();
		$out = [];

		for ($i = 0, $ien = count($result); $i < $ien; ++$i) {
			if ($this->_dbFormat) {
				$this->_dbFormat($result[$i]);
			}

			$out[$result[$i][$this->_dbPKey]] = $result[$i];
		}

		return $out;
	}

	/**
	 * Clean the database.
	 *
	 * @param Editor $editor Calling Editor instance
	 * @param Field  $field  Host field
	 *
	 * @internal
	 */
	public function dbCleanExec($editor, $field)
	{
		// Database and file system clean up BEFORE adding the new file to
		// the db, otherwise it will be removed immediately
		$tables = $editor->table();
		$this->_dbClean($editor->db(), $tables[0], $field->dbField());
	}

	/**
	 * Get the set error message.
	 *
	 * @return string Class error
	 *
	 * @internal
	 */
	public function error()
	{
		return $this->_error;
	}

	/**
	 * Execute an upload.
	 *
	 * @param Editor $editor Calling Editor instance
	 *
	 * @return int Primary key value
	 *
	 * @internal
	 */
	public function exec($editor)
	{
		$id = null;
		$upload = $_FILES['upload'];

		// Validation - PHP standard validation
		if ($upload['error'] !== \UPLOAD_ERR_OK) {
			if ($upload['error'] === \UPLOAD_ERR_INI_SIZE) {
				$this->_error = 'File exceeds maximum file upload size';
			} else {
				$this->_error = 'There was an error uploading the file (' . $upload['error'] . ')';
			}

			return false;
		}

		// Validation - acceptable file extensions
		if (is_array($this->_extns)) {
			$extn = pathinfo($upload['name'], \PATHINFO_EXTENSION);

			if (in_array(strtolower($extn), array_map('strtolower', $this->_extns)) === false) {
				$this->_error = $this->_extnError;

				return false;
			}
		}

		// Validation - custom callback
		for ($i = 0, $ien = count($this->_validators); $i < $ien; ++$i) {
			$res = $this->_validators[$i]($upload);

			if (is_string($res)) {
				$this->_error = $res;

				return false;
			}
		}

		// Database
		if ($this->_dbTable) {
			foreach ($this->_dbFields as $column => $prop) {
				// We can't know what the path is, if it has moved into place
				// by an external function - throw an error if this does happen
				if (
					!is_string($this->_action)
					&& ($prop === self::DB_SYSTEM_PATH || $prop === self::DB_WEB_PATH)
				) {
					$this->_error = 'Cannot set path information in database ' .
						'if a custom method is used to save the file.';

					return false;
				}
			}

			// Commit to the database
			$id = $this->_dbExec($upload, $editor->db());
		}

		// Perform file system actions
		return $this->_actionExec($upload, $id);
	}

	/**
	 * Get the primary key column for the table.
	 *
	 * @return string Primary key column name
	 *
	 * @internal
	 */
	public function pkey()
	{
		return $this->_dbPKey;
	}

	/**
	 * Get the db table name.
	 *
	 * @return string DB table name
	 *
	 * @internal
	 */
	public function table()
	{
		return $this->_dbTable;
	}

	/*  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *
	 * Private methods
	 */

	/**
	 * Execute the configured action for the upload.
	 *
	 * @param array $upload $_FILES['upload']
	 * @param int   $id     Primary key value
	 *
	 * @return int File    identifier - typically the primary key
	 */
	private function _actionExec($upload, $id)
	{
		if (!is_string($this->_action)) {
			// Custom function
			$action = $this->_action;

			return $action($upload, $id);
		}

		// Default action - move the file to the location specified by the
		// action string
		$to = $this->_path($upload['name'], $id);
		$res = rename($upload['tmp_name'], $to);

		if ($res === false) {
			$this->_error = 'An error occurred while moving the uploaded file.';

			return false;
		}

		if ($this->_mode) {
			chmod($to, $this->_mode);
		}

		return $id !== null ?
			$id :
			$to;
	}

	/**
	 * Perform the database clean by first getting the information about the
	 * orphaned rows and then calling the callback function. The callback can
	 * then instruct the rows to be removed through the return value.
	 *
	 * @param Database $db          Database instance
	 * @param string   $editorTable Editor Editor instance table name
	 * @param string   $fieldName   Host field's name
	 */
	private function _dbClean($db, $editorTable, $fieldName)
	{
		$callback = $this->_dbCleanCallback;

		if (!$this->_dbTable || !$callback) {
			return;
		}

		// If there is a table / field that we should use to check if the value
		// is in use, then use that. Otherwise we'll try to use the information
		// from the Editor / Field instance.
		if ($this->_dbCleanTableField) {
			$fieldName = $this->_dbCleanTableField;
		}

		$a = explode('.', $fieldName);
		if (count($a) === 1) {
			$table = $editorTable;
			$field = $a[0];
		} elseif (count($a) === 2) {
			$table = $a[0];
			$field = $a[1];
		} else {
			$table = $a[1];
			$field = $a[2];
		}

		// Select the details requested, for the columns requested
		$q = $db
			->query('select')
			->table($this->_dbTable)
			->get($this->_dbPKey);

		foreach ($this->_dbFields as $column => $prop) {
			if ($prop !== self::DB_CONTENT) {
				$q->get($column);
			}
		}

		$q->where($this->_dbPKey, '(SELECT ' . $field . ' FROM ' . $table . '  WHERE ' . $field . ' IS NOT NULL)', 'NOT IN', false);

		$data = $q->exec()->fetchAll();

		if (count($data) === 0) {
			return;
		}

		$result = $callback($data);

		// Delete the selected rows, iff the developer says to do so with the
		// returned value (i.e. acknowledge that the files have be removed from
		// the file system)
		if ($result === true) {
			$qDelete = $db
				->query('delete')
				->table($this->_dbTable);

			for ($i = 0, $ien = count($data); $i < $ien; ++$i) {
				$qDelete->or_where($this->_dbPKey, $data[$i][$this->_dbPKey]);
			}

			$qDelete->exec();
		}
	}

	/**
	 * Add a record to the database for a newly uploaded file.
	 *
	 * @param array    $upload $_FILES['upload']
	 * @param Database $db     Database instance
	 *
	 * @return int Primary key value for the newly uploaded file
	 */
	private function _dbExec($upload, $db)
	{
		$pathFields = [];
		$insertedId = null;

		// Insert the details requested, for the columns requested
		$q = $db
			->query('insert')
			->table($this->_dbTable)
			->pkey($this->_dbPKey);

		foreach ($this->_dbFields as $column => $prop) {
			switch ($prop) {
				case self::DB_READ_ONLY:
					break;
				case self::DB_CONTENT:
					$q->set($column, file_get_contents($upload['tmp_name']));

					break;
				case self::DB_CONTENT_TYPE:
				case self::DB_MIME_TYPE:
					$finfo = finfo_open(\FILEINFO_MIME);
					$mime = finfo_file($finfo, $upload['tmp_name']);
					finfo_close($finfo);

					$q->set($column, $mime);

					break;
				case self::DB_EXTN:
					$extn = pathinfo($upload['name'], \PATHINFO_EXTENSION);
					$q->set($column, $extn);

					break;
				case self::DB_FILE_NAME:
					$q->set($column, $upload['name']);

					break;
				case self::DB_FILE_SIZE:
					$q->set($column, $upload['size']);

					break;
				case self::DB_SYSTEM_PATH:
					$pathFields[$column] = self::DB_SYSTEM_PATH;
					$q->set($column, '-'); // Use a temporary value to avoid cases

					break;                   // where the db will reject empty values
				case self::DB_WEB_PATH:
					$pathFields[$column] = self::DB_WEB_PATH;
					$q->set($column, '-'); // Use a temporary value (as above)

					break;
				default:
					$val = $prop;

					// Callable function - execute to get the value
					if ($prop instanceof \Closure) {
						$val = $prop($db, $upload);
					}

					// If the primary key value was set - use that
					if ($column === $this->_dbPKey) {
						$insertedId = $val;
					}

					if (is_string($val) && !empty($val)) {
						// Allow for replacement of __ID__, etc when the value is a string
						$pathFields[$column] = $val;
						$q->set($column, '-'); // Use a temporary value (as above)
					} else {
						$q->set($column, $val);
					}

					break;
			}
		}

		$res = $q->exec();
		$id = $insertedId !== null
			? $insertedId
			: $res->insertId();

		// Update the newly inserted row with the path information. We have to
		// use a second statement here as we don't know in advance what the
		// database schema is and don't want to prescribe that certain triggers
		// etc be created. It makes it a bit less efficient but much more
		// compatible
		if (count($pathFields)) {
			// For this to operate the action must be a string, which is
			// validated in the `exec` method
			$path = $this->_path($upload['name'], $id);
			$webPath = str_replace($_SERVER['DOCUMENT_ROOT'], '', $path);
			$q = $db
				->query('update')
				->table($this->_dbTable)
				->where($this->_dbPKey, $id);

			foreach ($pathFields as $column => $type) {
				if ($type === self::DB_WEB_PATH) {
					$q->set($column, $webPath);
				} elseif ($type === self::DB_SYSTEM_PATH) {
					$q->set($column, $path);
				} else {
					$q->set($column, str_replace('__ID__', $id, $type));
				}
			}

			$q->exec();
		}

		return $id;
	}

	/**
	 * Apply macros to a user specified path.
	 *
	 * @param string $name File path
	 * @param int    $id   Primary key value for the file
	 *
	 * @return string Resolved path
	 */
	private function _path($name, $id)
	{
		$extn = pathinfo($name, \PATHINFO_EXTENSION);

		$to = $this->_action;
		$to = str_replace('__NAME__', $name, $to);
		$to = str_replace('__ID__', $id, $to);
		$to = str_replace('__EXTN__', $extn, $to);

		return $to;
	}
}
