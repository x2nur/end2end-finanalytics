local key = vim.keymap.set

key('n', '<leader>\\', ':noh<CR>') -- clear search
key('n', 'U', '<C-R>') -- redo

-- Nvim-tree
key('n', '<leader>q', ':NvimTreeToggle<CR>', { silent = true }) -- Toggle 

-- Win
key('n', '<leader>w', '<c-w>') -- Focus file

-- Buf 
key('n', '<s-down>', '<c-e>')
key('n', '<s-up>', '<c-y>')
key('n', 'gf', ':lua vim.lsp.buf.format()<cr>', { silent = true }) -- format file

-- Fzf 
key('n', '<leader>f', ':FzfLua files<CR>', { silent = true }) -- files 
key('n', '<leader>b', ':FzfLua buffers<CR>', { silent = true }) -- buffers
key('n', '<leader>g', ':FzfLua live_grep_native<CR>', { silent = true }) -- grep 

-- Code companion 
key({'n', 'v'}, '<leader>c', ':CodeCompanionChat Toggle<cr>', { silent = true })
key({'n', 'v'}, '<leader>s', ':CodeCompanionActions<cr>', { silent = true })
key('v', '<leader>a', ':CodeCompanionChat Add<cr>', { silent = true })

-- MoltenInit "shared"
-- MoltenEvaluateOperator (motion)
-- MoltenDelete (cell)
-- MoltenInterrupt (Ctrl-c)

key('n', '<localleader>i', ':MoltenInit http://localhost:8888?token=sometokenhere<CR>')    
key('n', '<localleader>l', ':MoltenEvaluateLine<CR>')    
key('n', '<localleader>o', ':MoltenEvaluateOperator<CR>')    
key('n', '<localleader>r', ':MoltenReevaluateCell<CR>')    
key('n', '<localleader>v', ':<C-u>MoltenEvaluateVisual<CR>gv')    
key('n', '<localleader>d', ':MoltenDelete<CR>')    
key('n', '<localleader>c', ':MoltenInterrupt<CR>')
key('n', '<localleader>e', ':noautocmd MoltenEnterOutput<CR>')
