return {
	settings = {
		Lua = {
			runtime = {
				version = 'LuaJIT',
				path = vim.split(package.path, ';'), -- Include default Lua paths
			},
			workspace = {
				library = {
					[vim.fn.expand('$VIMRUNTIME/lua')] = true,
					[vim.fn.expand('$VIMRUNTIME/lua/vim/lsp')] = true,
				},
				checkThirdParty = false, -- Optional: disable checking third-party libraries
			},
			diagnostics = {
				globals = { "vim" }
			}
		}
	}
}
