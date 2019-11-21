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
            ->setHost($settings['hosts'])
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
    public  function getIndexList(string $indexName)
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
    public  function createIndex(string $indexName, array $settings = [], array $mappings = [])
    {
        $params   = [
            'index' => $indexName,
            'body'  => [
                'settings' => (!empty($settings))? $settings : $this->getDefaultIndexSettings()
            ]
        ];

        if (!empty($mappings)) {
            $params['body']['mappings'] = $mappings
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
    public  function deleteIndex(string $indexName)
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
    public  function getAlias(string $alias)
    {
        $params   = [
            'name' => $alias
        ];

        return $this->elasticSearchClient->indices()->getAlias($params);
    }

    /**
     * Add Alias
     *
     * @author Seon <keangsiang.pua@armonia-tech.com>
     * @param  string $indexName
     * @param  string $alias
     * @return void
     */
    public  function addAlias(string $indexName, string $alias)
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
    public  function removeAlias(string $indexName, string $alias)
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
    public  function addDoc(string $indexName, array $record = [], string $id = "", string $idField = 'id')
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
     * @return void
     */
    public  function addDocs(string $indexName, array $records = [], string $idField = 'id')
    {
        if (!count($records)){
            throw new \Exception('Elasticsearch.addDocs.noDocs: An empty records array was passed to the addDocs() method.');
        }

        if (count($records))
        {
            for($i = 0; $i < count($records); $i++) {
                $params['body'][] = [
                    'index' => [
                        '_index' => $indexName,
                        '_id'    => $records[$i][$idField]
                    ]
                ];

                $params['body'][] = $records[$i];
            }

            $this->elasticSearchClient->bulk($params);
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
    public  function deleteDoc(string $indexName, string $id)
    {
        $params = [
            'index' => $indexName
            'id'    => $id
        ];

        $this->elasticSearchClient->delete($params);
    }

    /**
     * Search
     *
     * @author Seon <keangsiang.pua@armonia-tech.com>
     * @param  string $indexName
     * @param  array  $query
     * @param  string $from
     * @param  string $indexName
     * @return array
     */
    public  function search(
        string $indexName, 
        array  $query = [],
        string $from  = 0,
        string $size  = 10,
        string $sort  = ["_score"]
    )
    {
        $params = [
            'index' => $indexName
            'body'  => [
                'query' => $query,
                'from'  => $from,
                'size'  => $size,
                'sort'  => $sort
            ]
        ];

        return $this->elasticSearchClient->search($params);
    }

}
