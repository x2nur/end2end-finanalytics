-- vim.opt.runtimepath:append(',' .. vim.fn.expand('%:p:h'))

require("options")
require("keymaps")
require("config.lazy")

vim.api.nvim_create_autocmd("FileType", {
	pattern = "lua",
	callback = function()
		vim.opt_local.tabstop = 2
		vim.opt_local.shiftwidth = 2
		vim.opt_local.softtabstop = 2
		vim.opt_local.expandtab = false
	end,
})
