<?php

namespace ArmoniaElasticSearch\Test;

use ArmoniaElasticSearch\ElasticSearchEngine;
use ArmoniaElasticSearch\ElasticSearchResult;
use Elasticsearch\Common\Exceptions\NoNodesAvailableException;
use PHPUnit\Framework\TestCase;

class ElasticSearchEngineTest extends TestCase {

    private $_engine;

    public function setUp(): void {
        $this->assertNotEmpty(getenv('UNIT_TEST_ELASTIC_SEARCH_HOST'), "Please set the env UNIT_TEST_ELASTIC_SEARCH_HOST");
        // Settings here

        $this->_engine = new ElasticSearchEngine([
            'hosts' => [getenv('UNIT_TEST_ELASTIC_SEARCH_HOST')],
            'retry' => 2
        ]);
    }

    private function getIndexName() {
        $this->assertNotEmpty(getenv('UNIT_TEST_ELASTIC_SEARCH_INDEX'), "Please set the env UNIT_TEST_ELASTIC_SEARCH_INDEX");
        return getenv('UNIT_TEST_ELASTIC_SEARCH_INDEX');
    }

    private function getMapping() {
        return [
            'dynamic' => 'strict',
            '_source' => [
                'enabled' => false
            ],
            'properties' => [
                'id' => [
                    'type' => 'integer'
                ]
            ]
        ];
    }

    public function testIsElasticSearchAvailable() {
        $this->assertTrue($this->_engine->isElasticSearchAvailable());

        $bad_host = new ElasticSearchEngine([
            'hosts' => ['google.com:80'],
            'retry' => 1
        ]);

        $this->assertFalse($bad_host->isElasticSearchAvailable());
    }

    public function testGetIndexList() {
        $empty_result = $this->_engine->getIndexList('not_exist_index_list_*');
        $this->assertIsArray($empty_result);
        $this->assertEmpty($empty_result);

        $valid_result = $this->_engine->getIndexList($this->getIndexName());
        $this->assertIsArray($empty_result);

        $this->assertArrayHasKey('index', $valid_result[0]);
        $this->assertArrayHasKey('status', $valid_result[0]);
        // Note we cannot assert index name, as it may be alias
    }

    public function testCreateIndex() {
        $index_name = 'armonia-elasticsearch-unittest-' . uniqid();
        $settings = [];
        $mappings = $this->getMapping();

        $result = $this->_engine->createIndex($index_name, $settings, $mappings);

        $this->assertNull($result); // returns null

        return $index_name;
    }

    /**
     * @depends testCreateIndex
     */
    public function testMapping($index_name) {
        $result = $this->_engine->getMapping($index_name);

        $result_mapping = reset($result)['mappings'];
        $this->assertEquals($this->getMapping(), $result_mapping);

        $new_mapping = $result_mapping;

        $new_mapping['properties']['value'] = [
            'type' => 'keyword'
        ];

        $this->_engine->putMapping($index_name, $new_mapping);

        $new_result = $this->_engine->getMapping($index_name);

        $new_result_mapping = reset($new_result)['mappings'];
        $this->assertEquals($new_mapping, $new_result_mapping);
    }

    /**
     * @depends testCreateIndex
     */
    public function testAddAlias($index_name) {

        $alias_name = 'armonia-elasticsearch-unittest-alias' . uniqid();
        $result = $this->_engine->addAlias($index_name, $alias_name);

        $this->assertNull($result); // returns null
        return $alias_name;
    }

    /**
     * @depends testAddAlias
     */
    public function testExistAlias($alias_name) {

        $this->assertFalse($this->_engine->existAlias("not_found_alias_ASDD"));
        $this->assertTrue($this->_engine->existAlias($alias_name));
        return $alias_name;
    }

    /**
     * @depends testExistAlias
     */
    public function testGetAlias($alias_name) {
        // Not found
        $not_found_alias = $this->_engine->getAlias("not_found_alias_ASDD");

        $this->assertIsArray($not_found_alias);
        $this->assertEmpty($not_found_alias);

        // Found
        $found_alias = $this->_engine->getAlias($alias_name);

        $this->assertIsArray($found_alias);
        $this->assertNotEmpty($found_alias);

        return [$alias_name, key($found_alias)];
    }

    /**
     * @depends testGetAlias
     */
    public function testRemoveAlias($alias_data) {
        $alias_name = $alias_data[0];
        $index_name = $alias_data[1];
        // Found
        $found_alias = $this->_engine->removeAlias($index_name, $alias_name);

        $this->assertFalse($this->_engine->existAlias($alias_name));

        return $index_name;
    }

    /**
     * @depends testRemoveAlias
     */
    public function testDeleteIndex($index_name) {
        $result = $this->_engine->deleteIndex($index_name);
        $this->assertNull($result); // returns null
    }

    public function testSearchSingle() {
        $query = [];
        $query['bool']['filter'][] = [
            'terms' => [
                'id' => [10010000]
            ]
        ];

        $result = $this->_engine->search(
                $this->getIndexName(),
                [],
                $query
        );

        $this->assertArrayNotHasKey('_scroll_id', $result);

        $this->assertEquals(1, $result['hits']['total']['value'], "Did the id exist in test index?");
        $this->assertEquals(10010000, $result['hits']['hits'][0]['_source']['id']);

        $this->assertArrayHasKey('order_number', $result['hits']['hits'][0]['_source']);
        $this->assertArrayHasKey('order_date', $result['hits']['hits'][0]['_source']);
    }

    public function testSearchNoCondition() {
        $query = [];
        $result = $this->_engine->search(
                $this->getIndexName(),
                ['id', 'order_number'],
                $query
        );

        $this->assertGreaterThan(1, $result['hits']['total']['value']);
        $this->assertIsInt($result['hits']['hits'][0]['_source']['id']);

        $this->assertArrayHasKey('order_number', $result['hits']['hits'][0]['_source']);
        $this->assertArrayNotHasKey('order_date', $result['hits']['hits'][0]['_source']);
    }

    public function testSearchAllStandardParam() {
        $query = [];
        $query['bool']['filter'][] = [
            'terms' => [
                'marketplace_id' => [1]
            ]
        ];

        $result = $this->_engine->search(
                $this->getIndexName(),
                ['id', 'order_number'],
                $query,
                10,
                5,
                ['id']
        );

        $this->assertGreaterThan(1, $result['hits']['total']['value']);

        $this->assertArrayHasKey('order_number', $result['hits']['hits'][0]['_source']);
        $this->assertArrayNotHasKey('order_date', $result['hits']['hits'][0]['_source']);

        // returns sort when sort is given
        $this->assertArrayHasKey('sort', $result['hits']['hits'][0]);
        $this->assertEquals($result['hits']['hits'][0]['_source']['id'], $result['hits']['hits'][0]['sort'][0]);

        return $result;
    }

    /**
     * @depends testSearchAllStandardParam
     */
    public function testSearchAfter($prev_result) {
        $query = [];
        $query['bool']['filter'][] = [
            'terms' => [
                'marketplace_id' => [1]
            ]
        ];

        $prev_result_id = $prev_result['hits']['hits'][0]['_source']['id'];
        $prev_result_sort = $prev_result['hits']['hits'][0]['sort'];
        $result = $this->_engine->search(
                $this->getIndexName(),
                ['id', 'order_number'],
                $query,
                0,
                5,
                ['id'],
                [],
                '',
                $prev_result_sort
        );

        $this->assertGreaterThan(1, $result['hits']['total']['value']);

        $this->assertArrayHasKey('order_number', $result['hits']['hits'][0]['_source']);
        $this->assertArrayNotHasKey('order_date', $result['hits']['hits'][0]['_source']);

        // returns sort when sort is given
        $this->assertArrayHasKey('sort', $result['hits']['hits'][0]);
        $this->assertEquals($result['hits']['hits'][0]['_source']['id'], $result['hits']['hits'][0]['sort'][0]);

        $this->assertGreaterThan($prev_result_id, $result['hits']['hits'][0]['_source']['id']);
    }

    public function testSearchAggs() {
        $query = [];
        $aggs = [
            "test_aggs" => [
                "terms" => [
                    "field" => "marketplace_id"
                ]
            ]
        ];

        $result = $this->_engine->search(
                $this->getIndexName(),
                ['order_number'],
                $query,
                10,
                5,
                ['id'],
                $aggs
        );

        $this->assertGreaterThan(1, $result['hits']['total']['value']);

        $this->assertArrayHasKey('order_number', $result['hits']['hits'][0]['_source']);
        $this->assertArrayNotHasKey('order_date', $result['hits']['hits'][0]['_source']);

        $this->assertArrayHasKey('aggregations', $result);
        $this->assertArrayHasKey('test_aggs', $result['aggregations']);
    }

    public function testSearchScroll() {
        $query = [];
        $query['bool']['filter'][] = [
            'terms' => [
                'marketplace_id' => [1]
            ]
        ];

        $result = $this->_engine->search(
                $this->getIndexName(),
                ['id', 'order_number'],
                $query,
                0, // from must be 0
                5,
                ['id'],
                [], // aggs
                '1m' // scroll
        );

        $this->assertArrayHasKey('_scroll_id', $result);
        $scroll_id = $result['_scroll_id'];

        $this->assertGreaterThan(1, $result['hits']['total']['value']);
        $this->assertArrayHasKey('id', $result['hits']['hits'][0]['_source']);
        $this->assertArrayHasKey('order_number', $result['hits']['hits'][0]['_source']);

        $first_page_id = $result['hits']['hits'][0]['_source']['id'];

        $scroll_result_1 = $this->_engine->scroll($scroll_id);

        $this->assertGreaterThan(1, $scroll_result_1['hits']['total']['value']);
        $this->assertArrayHasKey('id', $scroll_result_1['hits']['hits'][0]['_source']);
        $this->assertArrayHasKey('order_number', $scroll_result_1['hits']['hits'][0]['_source']);

        $scroll_result_1_id = $scroll_result_1['hits']['hits'][0]['_source']['id'];

        $scroll_result_2 = $this->_engine->scroll($scroll_id);

        $this->assertGreaterThan(1, $scroll_result_2['hits']['total']['value']);
        $this->assertArrayHasKey('id', $scroll_result_2['hits']['hits'][0]['_source']);
        $this->assertArrayHasKey('order_number', $scroll_result_2['hits']['hits'][0]['_source']);

        $scroll_result_2_id = $scroll_result_2['hits']['hits'][0]['_source']['id'];

        $this->assertGreaterThan($first_page_id, $scroll_result_1_id);
        $this->assertGreaterThan($scroll_result_1_id, $scroll_result_2_id);
    }
}
