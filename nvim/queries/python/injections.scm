; extends

; (expression_statement
;   (call 
;     function: (attribute
; 	  object: (identifier)
;       attribute: (identifier) @_method (#eq? @_method "sql") ) 
;     arguments: (argument_list
;       (string
;         (string_content) @injection.content  
; 	  )
; 	)
;   )
;   (#offset! @injection.content 0 1 0 -1)
;   (#set! injection.language "sql") 
;   ; (#set! injection.include-children)
; )
;
;
; ((string (string_content) @injection.content)
;   (#match? @injection.content "(?i)select\\s+.*from")
;   (#set! injection.language "sql")
;   (#set! injection.include-children))

(call
  function: (identifier) @meth (#eq? @meth "sql") 
  arguments: (argument_list
    (string
	  (string_content) @injection.content) )
  (#set! injection.language "sql")
  (#set! injection.include-children)
)
