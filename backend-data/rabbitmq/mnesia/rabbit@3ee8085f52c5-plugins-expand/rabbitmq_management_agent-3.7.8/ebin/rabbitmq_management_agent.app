{application, 'rabbitmq_management_agent', [
	{description, "RabbitMQ Management Agent"},
	{vsn, "3.7.8"},
	{id, "v3.7.8"},
	{modules, ['Elixir.RabbitMQ.CLI.Ctl.Commands.ResetStatsDbCommand','exometer_slide','rabbit_mgmt_agent_app','rabbit_mgmt_agent_config','rabbit_mgmt_agent_sup','rabbit_mgmt_agent_sup_sup','rabbit_mgmt_data','rabbit_mgmt_db_handler','rabbit_mgmt_external_stats','rabbit_mgmt_format','rabbit_mgmt_gc','rabbit_mgmt_metrics_collector','rabbit_mgmt_metrics_gc','rabbit_mgmt_storage']},
	{registered, [rabbitmq_management_agent_sup]},
	{applications, [kernel,stdlib,xmerl,mnesia,ranch,ssl,crypto,public_key,rabbit_common,rabbit]},
	{mod, {rabbit_mgmt_agent_app, []}},
	{env, [
	    {rates_mode,        basic},
	    {sample_retention_policies,
	     %% List of {MaxAgeInSeconds, SampleEveryNSeconds}
	     [{global,   [{605, 5}, {3660, 60}, {29400, 600}, {86400, 1800}]},
	      {basic,    [{605, 5}, {3600, 60}]},
	      {detailed, [{605, 5}]}]}
	  ]},
		{broker_version_requirements, []}
]}.