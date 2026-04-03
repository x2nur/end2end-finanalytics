return {
	'rebelot/kanagawa.nvim',
	cond = true,
	priority = 1000,
	lazy = false, 
	config = function()
		vim.cmd(':colorscheme kanagawa-dragon')
	end,
	opts = {
		theme = 'dragon',
		keywordStyle = { italic = false, bold = true },
		background = {
			dark = 'dragon',
		},
		overrides = function()
			return {
				["@variable.builtin"] = { italic = false },
			}
		end,
		colors = {
			palette = {
				--dragonViolet = '#DE5D83',
				dragonRed = '#E0B0FF'
			},
		}
	}
}
