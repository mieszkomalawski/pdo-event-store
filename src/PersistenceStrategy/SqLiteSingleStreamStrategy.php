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

namespace Prooph\EventStore\Pdo\PersistenceStrategy;

use Iterator;
use Prooph\EventStore\Pdo\HasQueryHint;
use Prooph\EventStore\Pdo\PersistenceStrategy;
use Prooph\EventStore\StreamName;

final class SqLiteSingleStreamStrategy implements PersistenceStrategy
{
    /**
     * @param string $tableName
     * @return string[]
     */
    public function createSchema(string $tableName): array
    {
        $statement = <<<EOT
CREATE TABLE `$tableName` (
    `no`  INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE,
    `event_id` CHAR(36) NOT NULL UNIQUE,
    `event_name` VARCHAR(100) NOT NULL,
    `payload` JSON NOT NULL,
    `metadata` JSON NOT NULL,
    `created_at` DATETIME(6) NOT NULL,
    `aggregate_version` INTEGER NOT NULL,
    `aggregate_id` CHAR(36)  NOT NULL,
    `aggregate_type` VARCHAR(150) NOT NULL,
	    UNIQUE (`aggregate_type`, `aggregate_id`, `aggregate_version` ),
	    UNIQUE (`aggregate_type`, `aggregate_id`, `no` )
)
 ;
EOT;

        return [$statement];
    }

    public function columnNames(): array
    {
        return [
            'event_id',
            'event_name',
            'payload',
            'metadata',
            'created_at',
            'aggregate_version',
            'aggregate_id',
            'aggregate_type'
        ];
    }

    public function prepareData(Iterator $streamEvents): array
    {
        $data = [];

        foreach ($streamEvents as $event) {
            $data[] = $event->uuid()->toString();
            $data[] = $event->messageName();
            $data[] = json_encode($event->payload());
            $data[] = json_encode($event->metadata());
            $data[] = $event->createdAt()->format('Y-m-d\TH:i:s.u');
            $data[] = $event->metadata()['_aggregate_version'];
            $data[] = $event->metadata()['_aggregate_id'];
            $data[] = $event->metadata()['_aggregate_type'];
        }

        return $data;
    }

    public function generateTableName(StreamName $streamName): string
    {
        return '_' . sha1($streamName->toString());
    }

    public function indexName(): string
    {
        return 'ix_query_aggregate';
    }
}
