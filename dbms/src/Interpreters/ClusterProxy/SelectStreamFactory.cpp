#include <Interpreters/ClusterProxy/SelectStreamFactory.h>
#include <Interpreters/ClusterProxy/ShardWithLocalReplicaBlockInputStream.h>
#include <DataStreams/RemoteBlockInputStream.h>

namespace DB
{

namespace ClusterProxy
{

SelectStreamFactory::SelectStreamFactory(
        QueryProcessingStage::Enum processed_stage_,
        QualifiedTableName main_table_,
        const Tables & external_tables_)
    : processed_stage{processed_stage_}
    , main_table(std::move(main_table_))
    , external_tables{external_tables_}
{
}

void SelectStreamFactory::createForShard(
        const Cluster::ShardInfo & shard_info,
        const String & query, const ASTPtr & query_ast,
        const Context & context, const ThrottlerPtr & throttler,
        BlockInputStreams & res)
{
    if (shard_info.isLocal())
        res.emplace_back(std::make_shared<ShardWithLocalReplicaBlockInputStream>(query_ast, main_table, context, processed_stage));
    else
    {
        auto stream = std::make_shared<RemoteBlockInputStream>(shard_info.pool, query, context, nullptr, throttler, external_tables, processed_stage);
        stream->setPoolMode(PoolMode::GET_MANY);
        stream->setMainTable(main_table);
        res.emplace_back(std::move(stream));
    }
}

}
}
