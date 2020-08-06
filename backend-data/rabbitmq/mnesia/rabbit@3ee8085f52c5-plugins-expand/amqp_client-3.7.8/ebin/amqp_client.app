{application, 'amqp_client', [
	{description, "RabbitMQ AMQP Client"},
	{vsn, "3.7.8"},
	{id, "v3.7.8"},
	{modules, ['amqp_auth_mechanisms','amqp_channel','amqp_channel_sup','amqp_channel_sup_sup','amqp_channels_manager','amqp_client','amqp_connection','amqp_connection_sup','amqp_connection_type_sup','amqp_direct_connection','amqp_direct_consumer','amqp_gen_connection','amqp_gen_consumer','amqp_main_reader','amqp_network_connection','amqp_rpc_client','amqp_rpc_server','amqp_selective_consumer','amqp_ssl','amqp_sup','amqp_uri','amqp_util','rabbit_routing_util','uri_parser']},
	{registered, [amqp_client_sup,amqp_sup]},
	{applications, [kernel,stdlib,xmerl,rabbit_common]},
	{mod, {amqp_client, []}},
	{env, [
	    {prefer_ipv6, false},
	    {ssl_options, []}
	  ]},
	%% Hex.pm package informations.
	{maintainers, [
	    "RabbitMQ Team <info@rabbitmq.com>",
	    "Jean-Sebastien Pedron <jean-sebastien@rabbitmq.com>"
	  ]},
	{licenses, ["MPL 1.1"]},
	{links, [
	    {"Website", "http://www.rabbitmq.com/"},
	    {"GitHub", "https://github.com/rabbitmq/rabbitmq-erlang-client"},
	    {"User guide", "http://www.rabbitmq.com/erlang-client-user-guide.html"}
	  ]},
	{build_tools, ["make", "rebar3"]},
	{files, [
	    	    "erlang.mk",
	    "git-revisions.txt",
	    "include",
	    "LICENSE*",
	    "Makefile",
	    "rabbitmq-components.mk",
	    "README",
	    "README.md",
	    "src"
	  ]}
]}.