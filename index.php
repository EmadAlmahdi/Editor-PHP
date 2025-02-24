<?php declare(strict_types=1);
use DataTables\Database;
use DataTables\Editor;
use DataTables\Editor\Field;
use DataTables\Editor\Format;
use DataTables\Editor\Validate;
use DataTables\Editor\ValidateOptions;

require_once 'vendor/autoload.php';

$db = new Database([
    'type' => 'Mysql',
    'user' => 'root',
    'pass' => 'root',
    'host' => 'localhost',
    'db' => 'dt'
]);

// Antag att $db är en instans av din klass som innehåller update()-metoden

// $table = 'users_test2';
// $set = ['name' => "Emadov!"];
// $where = ['id' => 1];

// $updatedRows = $db->update($table, $set, $where);

// if ($updatedRows > 0) {
//     echo "User updated successfully!";
// } else {
//     echo "No user found to update.";
// }


// dd($db->selectDistinct("users_test1", 'name'));



// Build our Editor instance and process the data coming from _POST
new Editor($db, 'datatables_demo')
    ->fields(
        Field::inst('first_name')
            ->validator(Validate::notEmpty(
                ValidateOptions::inst()
                    ->message('A first name is required')
            )),
        Field::inst('last_name')
            ->validator(Validate::notEmpty(
                ValidateOptions::inst()
                    ->message('A last name is required')
            )),
        Field::inst('position'),
        Field::inst('email')
            ->validator(Validate::email(
                ValidateOptions::inst()
                    ->message('Please enter an e-mail address')
            )),
        Field::inst('office'),
        Field::inst('extn'),
        Field::inst('age')
            ->validator(Validate::numeric())
            ->setFormatter(Format::ifEmpty(null)),
        Field::inst('salary')
            ->validator(Validate::numeric())
            ->setFormatter(Format::ifEmpty(null)),
        Field::inst('start_date')
            ->validator(Validate::dateFormat('Y-m-d'))
            ->getFormatter(Format::dateSqlToFormat('Y-m-d'))
            ->setFormatter(Format::dateFormatToSql('Y-m-d'))
    )
    ->debug(true)
    ->process($_POST)
    ->json(true, JSON_PRETTY_PRINT);

