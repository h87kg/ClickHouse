#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>
#include <Interpreters/Cluster.h>
#include <Parsers/IAST.h>
#include <Core/QualifiedTableName.h>
#include <Interpreters/Context.h>
#include <Common/Throttler.h>
#include <Core/QueryProcessingStage.h>

#include <common/logger_useful.h>

#include <memory>
#include <mutex>

namespace DB
{
namespace ClusterProxy
{

class ShardWithLocalReplicaBlockInputStream : public IProfilingBlockInputStream
{
public:
    ShardWithLocalReplicaBlockInputStream(
            const Cluster::ShardInfo & shard_info,
            const String & query_, const ASTPtr & query_ast_, const QualifiedTableName & main_table_,
            const Context & context_, const ThrottlerPtr & throttler_,
            const Tables & external_tables_,
            QueryProcessingStage::Enum to_stage_)
        : query(query_), query_ast(query_ast_), main_table(main_table_)
        , context(context_), throttler(throttler_)
        , external_tables(external_tables_)
        , to_stage(to_stage_)
    {
    }

    void readPrefix() override;

    void cancel() override;

protected:

    Block readImpl() override;

    String getName() const override { return "ShardWithLocalReplica"; }

    String getID() const override
    {
        std::stringstream res;
        res << "ShardWithLocalReplica(" << this << ")";
        return res.str();
    }

private:
    Logger * log = &Logger::get("ShardWithLocalReplicaBlockInputStream");

    String query;
    ASTPtr query_ast;
    QualifiedTableName main_table;
    Context context;
    ThrottlerPtr throttler;
    Tables external_tables;
    QueryProcessingStage::Enum to_stage;

    ConnectionPoolWithFailoverPtr connection_pool = nullptr;

    std::mutex cancel_mutex;
    std::unique_ptr<IProfilingBlockInputStream> impl;
};

}
}
