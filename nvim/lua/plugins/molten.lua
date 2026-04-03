return {
	"benlubas/molten-nvim",
	version = "^1.0.0",
	build = ":UpdateRemotePlugins",
	init = function() 
		vim.g.molten_virt_text_output = true
		vim.g.molten_cover_empty_lines = true
		vim.g.molten_auto_open_output = false
	end
}

