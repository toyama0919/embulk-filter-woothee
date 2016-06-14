package org.embulk.filter;

import java.util.List;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.Column;
import org.embulk.spi.FilterPlugin;
import org.embulk.spi.PageOutput;
import org.embulk.spi.Schema;
import org.embulk.spi.type.Types;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;

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
        
        Schema outputSchema = buildOutputSchema(task, inputSchema);

        control.run(task.dump(), outputSchema);
    }

    /**
     * @param task
     * @param inputSchema
     * @return
     */
    private Schema buildOutputSchema(PluginTask task, Schema inputSchema) {
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
        return outputSchema;
    }

    @Override
    public PageOutput open(TaskSource taskSource, final Schema inputSchema, final Schema outputSchema, final PageOutput output)
    {
        return new WootheePageOutput(taskSource, inputSchema, outputSchema, output);
    }
}
