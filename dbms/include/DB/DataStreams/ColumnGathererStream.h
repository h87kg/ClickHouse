#pragma once

#include <DB/DataStreams/IProfilingBlockInputStream.h>
#include <DB/Storages/IStorage.h>
#include <DB/Common/PODArray.h>


namespace DB
{


struct __attribute__((__packed__)) RowSourcePart
{
	/// Sequence of members is important to use RowSourcePart * as UInt8 * if flag = false
	UInt8 source_id: 7;
	UInt8 flag: 1;

	RowSourcePart() = default;

	RowSourcePart(unsigned source_id_, bool flag_ = false)
	{
		source_id = source_id_;
		flag = flag_;
	}

	static constexpr size_t MAX_PARTS = 127;
};

using MergedRowSources = PODArray<RowSourcePart>;


/** Gather single stream from multiple streams according to streams mask.
  * Stream mask maps row number to index of source stream.
  * Streams should conatin exactly one column.
  */
class ColumnGathererStream : public IProfilingBlockInputStream
{
public:
	ColumnGathererStream(const BlockInputStreams & source_streams, const String & column_name_,
						 const MergedRowSources & pos_to_source_idx_, size_t block_size_ = DEFAULT_BLOCK_SIZE);

	String getName() const override { return "ColumnGatherer"; }

	String getID() const override;

	Block readImpl() override;

private:

	String name;
	ColumnWithTypeAndName column;
	const MergedRowSources & pos_to_source_idx;

	/// Cache required fileds
	struct Source
	{
		const IColumn * column;
		size_t pos;
		size_t size;
		Block block;

		Source(Block && block_, const String & name) : block(std::move(block_))
		{
			update(name);
		}

		void update(const String & name)
		{
			column = block.getByName(name).column.get();
			size = block.rows();
			pos = 0;
		}
	};

	std::vector<Source> sources;

	size_t pos_global = 0;
	size_t block_size;

	Logger * log = &Logger::get("ColumnGathererStream");
};

}