Embulk::JavaPlugin.register_filter(
  "woothee", "org.embulk.filter.WootheeFilterPlugin",
  File.expand_path('../../../../classpath', __FILE__))
