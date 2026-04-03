local g = vim.g
local o = vim.opt

-- disable netrw
g.loaded_netrw = 1
g.loaded_netrwPlugin = 1

-- python3 venv for remote plugins
-- g.python3_host_prog=vim.fn.expand("~/.virtualenvs/neovim/bin/python3")

-- main modifier
g.mapleader = " "
g.maplocalleader = "\\"

-- colorscheme
o.termguicolors = true
o.background="dark"

-- global clipboard
o.clipboard = "unnamedplus"

-- appearance
o.relativenumber = true
o.scrolloff = 4
o.autowrite = true
o.cursorline = true
o.autoread = true
o.updatetime = 1000


-- search
o.ignorecase = true
o.smartcase = true -- can use uppercase in search term

-- hide various modes line text
--o.showmode = false

-- visually replace tab by spaces
o.tabstop = 4
o.softtabstop = 4
o.shiftwidth = 4 -- << and >>

-- activate 3 settings above
o.smarttab = true

-- do not write spaces instead tab
o.expandtab = false

-- autoindent in programming code
o.smartindent = true

-- place info icon on separate column
o.signcolumn = "yes" 

-- wrap by words, not symbols
o.wrap = true
o.linebreak = true
o.whichwrap = "<,>,h,l" 


