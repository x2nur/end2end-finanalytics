return {
	'nvim-treesitter/nvim-treesitter',
	branch = 'master',
	lazy = false,
	cond = true, 
	build = ':TSUpdate',
	config = function() 
		local ts = require('nvim-treesitter.configs')
		ts.setup {
			ensure_installed = { 
				'lua',
				'vim',
				'python',
				'sql', 
				'c', 
				'vimdoc', 
				'query', 
				'markdown', 
				'markdown_inline',
				'yaml',
			},
			sync_install = false,
			auto_install = false,
			highlight = {
				enable = true, 
			}
		} -- setup
	end -- config func
}
