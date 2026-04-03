return {
	'neovim/nvim-lspconfig',
	cond = true, 
	config = function() 
		vim.diagnostic.config({
			underline = false,
			--signs = false,
			severity_sort = true,
			float = {
				header = '',
				border = 'rounded',
				source = 'always',
			}
		})

		-- python lsp
		vim.lsp.enable('basedpyright')

		-- lua lsp
		vim.lsp.enable('lua_ls')
	end
}
