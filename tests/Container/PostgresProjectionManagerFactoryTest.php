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

namespace ProophTest\EventStore\Pdo\Container;

use PHPUnit\Framework\TestCase;
use Prooph\EventStore\EventStore;
use Prooph\EventStore\Pdo\Container\PostgresProjectionManagerFactory;
use Prooph\EventStore\Pdo\Projection\PostgresProjectionManager;
use ProophTest\EventStore\Pdo\TestUtil;
use Psr\Container\ContainerInterface;

/**
 * @group pdo_mysql
 */
class PostgresProjectionManagerFactoryTest extends TestCase
{
    /**
     * @test
     */
    public function it_creates_service(): void
    {
        $config['prooph']['projection_manager']['default'] = [
            'connection' => 'my_connection',
        ];

        $connection = TestUtil::getConnection();

        $container = $this->prophesize(ContainerInterface::class);
        $eventStore = $this->prophesize(EventStore::class);

        $container->get('my_connection')->willReturn($connection)->shouldBeCalled();
        $container->get('event_store')->willReturn($eventStore)->shouldBeCalled();
        $container->get('config')->willReturn($config)->shouldBeCalled();

        $factory = new PostgresProjectionManagerFactory();
        $projectionManager = $factory($container->reveal());

        $this->assertInstanceOf(PostgresProjectionManager::class, $projectionManager);
    }
}