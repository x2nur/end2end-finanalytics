return {
	'nvim-tree/nvim-tree.lua',
	dependencies = {
		{ 'nvim-tree/nvim-web-devicons' }
	},
	opts = {
		open_on_tab = true,
		view = {
			--hide_root_folder = true,
			adaptive_size = true,
		},
		renderer = {
			root_folder_label = false 
		},
		update_focused_file = {
			enable = true,
		}
	}
}
