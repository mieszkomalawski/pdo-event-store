<?php
/**
 * This file is part of the prooph/pdo-event-store.
 * (c) 2016-2017 prooph software GmbH <contact@prooph.de>
 * (c) 2016-2017 Sascha-Oliver Prolic <saschaprolic@googlemail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace ProophTest\EventStore\Pdo;

use PDO;

abstract class SqLiteTestUtil extends TestUtil
{

    public static function getConnection(): PDO
    {
        if (! isset(self::$connection)) {
            self::$connection = new PDO('sqlite:memory:', null, null, [
                PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION
                ]);
        }

        try {
            self::$connection->rollBack();
        } catch (\PDOException $e) {
            // ignore
        }

        return self::$connection;
    }

    protected static function hasRequiredConnectionParams(): bool
    {
        return true;
    }

    protected static function getSpecifiedConnectionParams(): array
    {
        return [];
    }

}
