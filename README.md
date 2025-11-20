# ffffff

kv da pizzaria

```

<key> ::= ([a-z] | [A-z] | [0-9] | "." | "-" | ":")+
<whitespace> ::= " "+
<separator> ::= "\r"

/* Qualquer coisa menos \r */

<value> ::= [a-z]
<command> ::= "read" <whitespace> <key>
            | "write" <whitespace> <key> "|" <value>
            | "delete" <whitespace> <key>
            | "status"
            | "keys"
            | "reads" <whitespace> <key>
<commands> ::= (<command> <separator>)* <command>

COMANDOS:
write -> success | error (se por algum motivo falhar)
read -> valor | error
delete -> success (se presente) | error (se a chave n existir)
status -> well going our operation
keys -> todas as chaves, separadas por \r
reads -> valores que comecem com o prefixo, separados por \r
comando inválido -> error

```