package org.embulk.filter;

import is.tagomor.woothee.Classifier;

import java.util.List;
import java.util.Map;

import org.embulk.config.TaskSource;
import org.embulk.filter.WootheeFilterPlugin.PluginTask;
import org.embulk.spi.Column;
import org.embulk.spi.Exec;
import org.embulk.spi.Page;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.spi.type.Types;

import com.google.common.collect.Maps;

public class WootheePageOutput implements PageOutput
{
    private final PluginTask task;
    private final List<Column> outputColumns;
    private final List<Column> inputColumns;
    private final Map<String, Column> wootheeColumnMap;
    private final Column keyNameColumn;
    private final PageReader reader;
    private final PageBuilder builder;

    public WootheePageOutput(TaskSource taskSource, Schema inputSchema, Schema outputSchema, PageOutput output) {
        this.task = taskSource.loadTask(PluginTask.class);
        this.outputColumns = outputSchema.getColumns();
        this.inputColumns = inputSchema.getColumns();
        Map<String, Column> inputColumnMap = Maps.newHashMap();
        this.wootheeColumnMap = Maps.newHashMap();
        for (Column column : outputColumns) {
            if (!inputColumns.contains(column)) {
                wootheeColumnMap.put(column.getName(), column);
            } else {
                inputColumnMap.put(column.getName(), column);
            }
        }
        this.reader = new PageReader(inputSchema);
        this.builder = new PageBuilder(Exec.getBufferAllocator(), outputSchema, output);
        this.keyNameColumn = inputColumnMap.get(task.getKeyName());
    }

    @Override
    public void finish() {
        builder.finish();
    }

    @Override
    public void close() {
        builder.close();
    }

    @Override
    public void add(Page page) {
        reader.setPage(page);

        while (reader.nextRecord()) {
            String userAgentString = reader.isNull(keyNameColumn) ? null : reader.getString(keyNameColumn);
            Map<String, String> ua = Classifier.parse(userAgentString);
            setValue(builder, ua);
            if (task.getFilterCategories().isPresent()) {
                if (task.getFilterCategories().orNull().contains(ua.get("category"))) {
                    builder.addRecord();
                }
            } else if (task.getDropCategories().isPresent()) {
                if (!task.getDropCategories().orNull().contains(ua.get("category"))) {
                    builder.addRecord();
                }
            } else {
                builder.addRecord();
            }
        }
    }

    /**
     * @param builder
     */
    private void setValue(PageBuilder builder, Map<String, String> ua) {
        if (task.getMergeAgentInfo()) {
            builder.setString(wootheeColumnMap.get(task.getOutKeyOs()), ua.get("os"));
            builder.setString(wootheeColumnMap.get(task.getOutKeyName()), ua.get("name"));
            builder.setString(wootheeColumnMap.get(task.getOutKeyCategory()), ua.get("category"));
            builder.setString(wootheeColumnMap.get(task.getOutKeyVersion()), ua.get("version"));
            builder.setString(wootheeColumnMap.get(task.getOutKeyVendor()), ua.get("vendor"));
        }

        for (Column inputColumn: inputColumns) {
            if (reader.isNull(inputColumn)) {
                builder.setNull(inputColumn);
                continue;
            }
            if (Types.STRING.equals(inputColumn.getType())) {
                builder.setString(inputColumn, reader.getString(inputColumn));
            } else if (Types.BOOLEAN.equals(inputColumn.getType())) {
                builder.setBoolean(inputColumn, reader.getBoolean(inputColumn));
            } else if (Types.DOUBLE.equals(inputColumn.getType())) {
                builder.setDouble(inputColumn, reader.getDouble(inputColumn));
            } else if (Types.LONG.equals(inputColumn.getType())) {
                builder.setLong(inputColumn, reader.getLong(inputColumn));
            } else if (Types.TIMESTAMP.equals(inputColumn.getType())) {
                builder.setTimestamp(inputColumn, reader.getTimestamp(inputColumn));
            } else if (Types.JSON.equals(inputColumn.getType())) {
                builder.setJson(inputColumn, reader.getJson(inputColumn));
            }
        }
    }
}
