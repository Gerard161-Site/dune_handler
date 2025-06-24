connection_args = {
    'api_key': {
        'type': 'str',
        'description': 'Sim API key (starts with sim_)'
    },
    'base_url': {
        'type': 'str',
        'description': 'Sim API base URL',
        'default': 'https://api.sim.dune.com/v1'
    }
}

connection_args_example = {
    'api_key': 'sim_your_api_key_here',
    'base_url': 'https://api.sim.dune.com/v1'
} 