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

use ArrayIterator;
use PDO;
use Prooph\Common\Messaging\FQCNMessageFactory;
use Prooph\EventStore\Exception\ConcurrencyException;
use Prooph\EventStore\Metadata\FieldType;
use Prooph\EventStore\Metadata\MetadataMatcher;
use Prooph\EventStore\Metadata\Operator;
use Prooph\EventStore\Pdo\Exception\RuntimeException;
use Prooph\EventStore\Pdo\MySqlEventStore;
use Prooph\EventStore\Pdo\PersistenceStrategy\MySqlAggregateStreamStrategy;
use Prooph\EventStore\Pdo\PersistenceStrategy\MySqlSingleStreamStrategy;
use Prooph\EventStore\Pdo\PersistenceStrategy\SqLiteAggregateStreamStrategy;
use Prooph\EventStore\Pdo\PersistenceStrategy\SqLiteSingleStreamStrategy;
use Prooph\EventStore\Pdo\SqLiteEventStore;
use Prooph\EventStore\Stream;
use Prooph\EventStore\StreamName;
use ProophTest\EventStore\Mock\UserCreated;
use ProophTest\EventStore\Mock\UsernameChanged;
use Ramsey\Uuid\Uuid;

/**
 * @group mysql
 */
final class SqLiteEventStoreTest extends AbstractPdoEventStoreTest
{
    /**
     * @var SqLiteEventStore
     */
    protected $eventStore;

    protected function setUp(): void
    {
        putenv('DB_DRIVER=pdo_sqlite');
        putenv('DB=sqlite_3');
        if (SqLiteTestUtil::getDatabaseDriver() !== 'pdo_sqlite') {
            throw new \RuntimeException('Invalid database driver');
        }

        $this->connection = SqLiteTestUtil::getConnection();
        SqLiteTestUtil::initDefaultDatabaseTables($this->connection);

        $this->eventStore = new SqLiteEventStore(
            new FQCNMessageFactory(),
            $this->connection,
            new SqLiteAggregateStreamStrategy()
        );
    }

    /**
     * @test
     */
    public function it_cannot_create_new_stream_if_table_name_is_already_used(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Error during createSchemaFor');

        $streamName = new StreamName('foo');
        $strategy = new SqLiteAggregateStreamStrategy();
        $schema = $strategy->createSchema($strategy->generateTableName($streamName));

        foreach ($schema as $command) {
            $statement = $this->connection->prepare($command);
            $statement->execute();
        }

        $this->eventStore->create(new Stream($streamName, new ArrayIterator()));
    }

    /**
     * @test
     */
    public function it_loads_correctly_using_single_stream_per_aggregate_type_strategy(): void
    {
        $this->eventStore = new SqLiteEventStore(
            new FQCNMessageFactory(),
            $this->connection,
            new SqLiteSingleStreamStrategy(),
            5
        );

        $streamName = new StreamName('Prooph\Model\User');

        $stream = new Stream($streamName, new ArrayIterator($this->getMultipleTestEvents()));

        $this->eventStore->create($stream);

        $metadataMatcher = new MetadataMatcher();
        $metadataMatcher = $metadataMatcher->withMetadataMatch('aggregate_id', Operator::EQUALS(), 'one', FieldType::MESSAGE_PROPERTY());
        $events = iterator_to_array($this->eventStore->load($streamName, 1, null, $metadataMatcher));
        $this->assertCount(10, $events);
        $lastUser1Event = array_pop($events);

        $metadataMatcher = new MetadataMatcher();
        $metadataMatcher = $metadataMatcher->withMetadataMatch('aggregate_id', Operator::EQUALS(), 'two', FieldType::MESSAGE_PROPERTY());
        $events = iterator_to_array($this->eventStore->load($streamName, 1, null, $metadataMatcher));
        $this->assertCount(10, $events);
        $lastUser2Event = array_pop($events);

        $this->assertEquals('Sandro', $lastUser1Event->payload()['name']);
        $this->assertEquals('Bradley', $lastUser2Event->payload()['name']);
    }

    /**
     * @test
     */
    public function it_fails_to_write_with_duplicate_version_and_mulitple_streams_per_aggregate_strategy(): void
    {
        $this->expectException(ConcurrencyException::class);

        $this->eventStore = new SqLiteEventStore(
            new FQCNMessageFactory(),
            $this->connection,
            new SqLiteSingleStreamStrategy()
        );

        $streamEvent = UserCreated::with(
            ['name' => 'Max Mustermann', 'email' => 'contact@prooph.de'],
            1
        );

        $aggregateId = Uuid::uuid4()->toString();

        $streamEvent = $streamEvent->withAddedMetadata('tag', 'person');
        $streamEvent = $streamEvent->withAddedMetadata('_aggregate_id', $aggregateId);
        $streamEvent = $streamEvent->withAddedMetadata('_aggregate_type', 'user');

        $stream = new Stream(new StreamName('Prooph\Model\User'), new ArrayIterator([$streamEvent]));

        $this->eventStore->create($stream);

        $streamEvent = UsernameChanged::with(
            ['name' => 'John Doe'],
            1
        );

        $streamEvent = $streamEvent->withAddedMetadata('tag', 'person');
        $streamEvent = $streamEvent->withAddedMetadata('_aggregate_id', $aggregateId);
        $streamEvent = $streamEvent->withAddedMetadata('_aggregate_type', 'user');

        $this->eventStore->appendTo(new StreamName('Prooph\Model\User'), new ArrayIterator([$streamEvent]));
    }

    /**
     * @test
     */
    public function it_fetches_stream_categories(): void
    {
        $streamNames = [];

        try {
            for ($i = 0; $i < 5; $i++) {
                $streamNames[] = 'foo-' . $i;
                $streamNames[] = 'bar-' . $i;
                $streamNames[] = 'baz-' . $i;
                $streamNames[] = 'bam-' . $i;
                $streamNames[] = 'foobar-' . $i;
                $streamNames[] = 'foobaz-' . $i;
                $streamNames[] = 'foobam-' . $i;
                $this->eventStore->create(new Stream(new StreamName('foo-' . $i), new \EmptyIterator()));
                $this->eventStore->create(new Stream(new StreamName('bar-' . $i), new \EmptyIterator()));
                $this->eventStore->create(new Stream(new StreamName('baz-' . $i), new \EmptyIterator()));
                $this->eventStore->create(new Stream(new StreamName('bam-' . $i), new \EmptyIterator()));
                $this->eventStore->create(new Stream(new StreamName('foobar-' . $i), new \EmptyIterator()));
                $this->eventStore->create(new Stream(new StreamName('foobaz-' . $i), new \EmptyIterator()));
                $this->eventStore->create(new Stream(new StreamName('foobam-' . $i), new \EmptyIterator()));
            }

            for ($i = 0; $i < 20; $i++) {
                $streamName = uniqid('rand');
                $streamNames[] = $streamName;
                $this->eventStore->create(new Stream(new StreamName($streamName), new \EmptyIterator()));
            }

            $this->assertCount(7, $this->eventStore->fetchCategoryNames(null, 20, 0));
            $this->assertCount(0, $this->eventStore->fetchCategoryNames(null, 20, 20));
            $this->assertCount(3, $this->eventStore->fetchCategoryNames(null, 3, 0));
            $this->assertCount(3, $this->eventStore->fetchCategoryNames(null, 3, 3));
            $this->assertCount(5, $this->eventStore->fetchCategoryNames(null, 10, 2));

            $this->assertCount(1, $this->eventStore->fetchCategoryNames('foo', 20, 0));
        } finally {
            foreach ($streamNames as $streamName) {
                $this->eventStore->delete(new StreamName($streamName));
            }
        }
    }

    /**
     * @return Message[]
     */
    protected function getMultipleTestEvents(): array
    {
        $events = [];

        $event = UserCreated::with(['name' => 'Alex'], 1);
        $events[] = $event->withAddedMetadata('_aggregate_id', 'one')->withAddedMetadata('_aggregate_type', 'user');

        $event = UserCreated::with(['name' => 'Sascha'], 1);
        $events[] = $event->withAddedMetadata('_aggregate_id', 'two')->withAddedMetadata('_aggregate_type', 'user');

        for ($i = 2; $i < 10; $i++) {
            $event = UsernameChanged::with(['name' => uniqid('name_')], $i);
            $events[] = $event->withAddedMetadata('_aggregate_id', 'two')->withAddedMetadata('_aggregate_type', 'user');

            $event = UsernameChanged::with(['name' => uniqid('name_')], $i);
            $events[] = $event->withAddedMetadata('_aggregate_id', 'one')->withAddedMetadata('_aggregate_type', 'user');
        }

        $event = UsernameChanged::with(['name' => 'Sandro'], 100);
        $events[] = $event->withAddedMetadata('_aggregate_id', 'one')->withAddedMetadata('_aggregate_type', 'user');

        $event = UsernameChanged::with(['name' => 'Bradley'], 100);
        $events[] = $event->withAddedMetadata('_aggregate_id', 'two')->withAddedMetadata('_aggregate_type', 'user');

        return $events;
    }
}
