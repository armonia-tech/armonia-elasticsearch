<?php
namespace ArmoniaElasticSearch;

use Elasticsearch\ClientBuilder;

class ElasticSearchEngine
{
    private $elasticSearchSetting;
    private $elasticSearchClient;

    /**
     * Construct
     *
     * @author Seon <keangsiang.pua@armonia-tech.com>
     * @param  array $settings
     * @return void
     */
    public function __construct(array $settings)
    {
        $this->elasticSearchClient = ClientBuilder::create()
            ->setHosts($settings['hosts'])
            ->setRetries($settings['retry'])
            ->build();
    }

    /**
     * Get Index List
     *
     * @author Seon <keangsiang.pua@armonia-tech.com>
     * @param  string $indexName
     * @return void
     */
    public function getIndexList(string $indexName)
    {
        $params   = [
            'index' => $indexName,
        ];

        return $this->elasticSearchClient->cat()->indices($params);
    }

    /**
     * Create Index
     *
     * @author Seon <keangsiang.pua@armonia-tech.com>
     * @param  string $indexName
     * @param  array $settings
     * @param  array $mappings
     * @return void
     */
    public function createIndex(string $indexName, array $settings = [], array $mappings = [])
    {
        $params   = [
            'index' => $indexName,
            'body'  => [
                'settings' => (!empty($settings))? $settings : $this->_getDefaultIndexSettings()
            ]
        ];

        if (!empty($mappings)) {
            $params['body']['mappings'] = $mappings;
        }

        $this->elasticSearchClient->indices()->create($params);
    }

    /**
     * Delete Index
     *
     * @author Seon <keangsiang.pua@armonia-tech.com>
     * @param  string $indexName
     * @return void
     */
    public function deleteIndex(string $indexName)
    {
        $params   = [
            'index' => $indexName
        ];

        $this->elasticSearchClient->indices()->delete($params);
    }

    /**
     * Get Alias
     *
     * @author Seon <keangsiang.pua@armonia-tech.com>
     * @param  string $alias
     * @return array
     */
    public function getAlias(string $alias)
    {
        $params   = [
            'name' => $alias
        ];

        try {
            $alias = $this->elasticSearchClient->indices()->getAlias($params);
        } catch (\Exception $e) {
            if ($e->getCode() == 404) {
                return [];
            }

            throw new \Exception($e);
        }

        return $alias;
    }

    /**
     * Add Alias
     *
     * @author Seon <keangsiang.pua@armonia-tech.com>
     * @param  string $indexName
     * @param  string $alias
     * @return void
     */
    public function addAlias(string $indexName, string $alias)
    {
        $params   = [
            'body'  => [
                'actions' => [
                    [
                        'add' => [
                            'index' => $indexName,
                            'alias' => $alias
                        ]
                    ]
                ]
            ]
        ];

        $this->elasticSearchClient->indices()->updateAliases($params);
    }

    /**
     * Remove Alias
     *
     * @author Seon <keangsiang.pua@armonia-tech.com>
     * @param  string $indexName
     * @param  string $alias
     * @return void
     */
    public function removeAlias(string $indexName, string $alias)
    {
        $params   = [
            'body'  => [
                'actions' => [
                    [
                        'remove' => [
                            'index' => $indexName,
                            'alias' => $alias
                        ]
                    ]
                ]
            ]
        ];

        $this->elasticSearchClient->indices()->updateAliases($params);
    }
    
    /**
     * Add / Update Doc
     *
     * @author Seon <keangsiang.pua@armonia-tech.com>
     * @param  string $indexName
     * @param  array $record
     * @param  string $id default ''
     * @param  string $idField default 'id'
     * @return void
     */
    public function addDoc(string $indexName, array $record = [], string $id = "", string $idField = 'id')
    {
        $params = [
            'index' => $indexName
        ];

        //add / update document
        if (empty($id)) {
            $params['id']   = $record[$idField];
            $params['body'] = $record;

            $this->elasticSearchClient->index($params);
        } else {
            $params['id']          = $id;
            $params['body']['doc'] = $record;

            $this->elasticSearchClient->update($params);
        }
    }

    /**
     * Add Docs
     *
     * @author Seon <keangsiang.pua@armonia-tech.com>
     * @param  string $indexName
     * @param  array $records
     * @param  string $idField default 'id'
     * @return array
     */
    public function addDocs(string $indexName, array $records = [], string $idField = 'id')
    {
        if (!count($records)) {
            throw new \Exception('Elasticsearch.addDocs.noDocs: An empty records array was passed to the addDocs() method.');
        }

        if (count($records)) {
            for ($i = 0; $i < count($records); $i++) {
                $params['body'][] = [
                    'index' => [
                        '_index' => $indexName,
                        '_id'    => $records[$i][$idField]
                    ]
                ];

                $params['body'][] = $records[$i];
            }

            return $this->elasticSearchClient->bulk($params);
        }
    }

    /**
     * Delete Doc
     *
     * @author Seon <keangsiang.pua@armonia-tech.com>
     * @param  string $indexName
     * @param  string $id
     * @return void
     */
    public function deleteDoc(string $indexName, string $id)
    {
        $params = [
            'index' => $indexName,
            'id'    => $id
        ];

        return $this->elasticSearchClient->delete($params);
    }

    /**
     * Delete Doc By Query
     *
     * @author Seon <keangsiang.pua@armonia-tech.com>
     * @param  string $indexName
     * @param  array $body
     * @return void
     */
    public function deleteByQuery(string $indexName, array $body)
    {
        $params = [
            'index' => $indexName,
            'body'  => $body
        ];

        return $this->elasticSearchClient->deleteByQuery($params);
    }

    /**
     * Search
     *
     * @author Seon <keangsiang.pua@armonia-tech.com>
     * @param  string $indexName
     * @param  array  $query
     * @param  int $from
     * @param  int $size
     * @param  array $sort
     * @return array
     */
    public function search(
        string $indexName,
        array $source = [],
        array  $query = [],
        int $from = 0,
        int $size = 10,
        array $sort = ["_score"],
        array $aggs = []
    ) {
        $body = [
            'from'  => $from,
            'size'  => $size,
            'sort'  => $sort
        ];

        if (!empty($query)) {
            $body['query'] = $query;
        }

        if (!empty($aggs)) {
            $body['aggs'] = $aggs;
        }

        if (!empty($source)) {
            $body['_source'] = $source;
        }

        $params = [
            'index' => $indexName,
            'body'  => $body
        ];

        return $this->elasticSearchClient->search($params);
    }

    /**
     * Search
     *
     * @author Seon <keangsiang.pua@armonia-tech.com>
     * @return array
     */
    public function isElasticSearchAvailable()
    {
        $params = [];

        try {
            $response = $this->elasticSearchClient->ping($params);
        } catch (\Exception $e) {
            $response = false;
        }

        return $response;
    }

    private function _getDefaultIndexSettings()
    {
        return [
            'number_of_shards'   => 1,
            'number_of_replicas' => 0
        ];
    }
}
