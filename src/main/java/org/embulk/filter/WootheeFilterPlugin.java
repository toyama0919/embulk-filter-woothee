package org.embulk.filter;

import is.tagomor.woothee.Classifier;

import java.util.List;
import java.util.Map;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.Column;
import org.embulk.spi.Exec;
import org.embulk.spi.FilterPlugin;
import org.embulk.spi.Page;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.spi.type.Types;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

public class WootheeFilterPlugin implements FilterPlugin
{
    public interface PluginTask extends Task
    {
        @Config("key_name")
        public String getKeyName();

        @Config("out_key_name")
        @ConfigDefault("\"agent_name\"")
        public String getOutKeyName();
        
        @Config("out_key_category")
        @ConfigDefault("\"agent_category\"")
        public String getOutKeyCategory();
        
        @Config("out_key_os")
        @ConfigDefault("\"agent_os\"")
        public String getOutKeyOs();

        @Config("out_key_version")
        @ConfigDefault("\"agent_version\"")
        public String getOutKeyVersion();

        @Config("out_key_vendor")
        @ConfigDefault("\"agent_vendor\"")
        public String getOutKeyVendor();

        @Config("filter_categories")
        @ConfigDefault("null")
        public Optional<List<String>> getFilterCategories();

        @Config("drop_categories")
        @ConfigDefault("null")
        public Optional<List<String>> getDropCategories();

        @Config("merge_agent_info")
        @ConfigDefault("false")
        public Boolean getMergeAgentInfo();
    }

    @Override
    public void transaction(ConfigSource config, Schema inputSchema,
            FilterPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);
        
        ImmutableList.Builder<Column> builder = ImmutableList.builder();
        int i = 0;
        for (Column inputColumn: inputSchema.getColumns()) {
            Column outputColumn = new Column(i++, inputColumn.getName(), inputColumn.getType());
            builder.add(outputColumn);
        }
        if (task.getMergeAgentInfo()) {
            builder.add(new Column(i++, task.getOutKeyOs(), Types.STRING));
            builder.add(new Column(i++, task.getOutKeyName(), Types.STRING));
            builder.add(new Column(i++, task.getOutKeyCategory(), Types.STRING));
            builder.add(new Column(i++, task.getOutKeyVersion(), Types.STRING));
            builder.add(new Column(i++, task.getOutKeyVendor(), Types.STRING));
        }
        Schema outputSchema = new Schema(builder.build());

        control.run(task.dump(), outputSchema);
    }

    @Override
    public PageOutput open(TaskSource taskSource, final Schema inputSchema, final Schema outputSchema, final PageOutput output)
    {
        final PluginTask task = taskSource.loadTask(PluginTask.class);

        final List<Column> outputColumns = outputSchema.getColumns();
        final List<Column> inputColumns = inputSchema.getColumns();
        Map<String, Column> inputColumnMap = Maps.newHashMap();
        final Map<String, Column> wootheeColumnMap = Maps.newHashMap();
        for (Column column : outputColumns) {
            if (!inputColumns.contains(column)) {
                wootheeColumnMap.put(column.getName(), column);
            } else {
                inputColumnMap.put(column.getName(), column);
            }
        }
        final Column keyNameColumn = inputColumnMap.get(task.getKeyName());
        
        return new PageOutput() {
            private PageReader reader = new PageReader(inputSchema);
            private PageBuilder builder = new PageBuilder(Exec.getBufferAllocator(), outputSchema, output);

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
        };     
    }
}
