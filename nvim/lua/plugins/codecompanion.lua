return {
	"olimorris/codecompanion.nvim",
	version = "^19.0.0",
	opts = {
		adapters = {
			http = {
				openai = function()
					return require('codecompanion.adapters').extend('openai', {
						schema = {
							model = {
								default = 'openai/gpt-oss-120b',
								-- default = 'google/gemini-2.5-flash-lite',
							},
							reasoning_effort = { default = 'low', },
							-- temperature = { default = 0.5, },
							max_tokens = { default = 4000 },
						},
						env = {
							api_key = 'OPENROUTER_API_KEY'
						},
						url = 'https://openrouter.ai/api/v1/chat/completions',
					})
				end,
			},
		},
		interactions = {
			chat = {
				adapter = 'copilot',
				keymaps = {
					send = { 
						modes = { n = '<c-cr>', i = '<c-cr>' },
					},
				},
				opts = {
					completion_provider = 'cmp'
				}
			},
			inline = { adapter = 'copilot' },
			cmd = { adapter = 'copilot' },
			background = { adapter = 'copilot' },
		},
		display = {
			chat = {
				window = {
					layout = 'vertical',
					position = 'right',
					width = 0.4,
					relative = "editor",
				},
			},
			action_palette = {
				prompt = "Prompt: ",
				provider = "fzf_lua",
				opts = {
					show_preset_actions = true, 
					show_preset_prompts = true,
					title = "> ",          
				},
			},
			diff = {
				enabled = true,
			},
		}
	},
	dependencies = {
		"nvim-lua/plenary.nvim",
		"nvim-treesitter/nvim-treesitter",
	},
}
