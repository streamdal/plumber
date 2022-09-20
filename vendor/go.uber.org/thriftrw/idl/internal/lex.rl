%% machine thrift;

package internal

import (
    "errors"
    "fmt"
    "strconv"

    "go.uber.org/thriftrw/ast"
)

%%{
write data;

# Access state consistent across Lex() calls using the "lex" object.
access lex.;
variable p lex.p;
variable pe lex.pe;

}%%

type lexer struct {
    program *ast.Program

    line int
    lineStart int

    docstringStart int
    lastDocstring string
    linesSinceDocstring int

    nodePositions NodePositions

    errors []ParseError
    parseFailed bool

    // Ragel:
    p, pe, cs, ts, te, act int
    data []byte
}


func newLexer(data []byte) *lexer {
    lex := &lexer{
        line: 1,
        nodePositions: make(NodePositions, 0),
        parseFailed: false,
        data: data,
        p: 0,
        pe: len(data),
    }
    %% write init;
    return lex
}

func (lex *lexer) Lex(out *yySymType) int {
    var (
        reservedKeyword string

        eof = lex.pe
        tok = 0
    )

    %%{
       docstring =
            '/**' @{ lex.docstringStart = lex.p - 2 }
            (any* - (any* '*/' any*))
            '*/' @{
                lex.lastDocstring = string(lex.data[lex.docstringStart:lex.p + 1])
                lex.linesSinceDocstring = 0
            };

        ws = [ \t\r];

        # All uses of \n MUST use this instead if we want accurate line
        # number tracking.
        newline = '\n' >{
            lex.line++
            lex.lineStart = lex.p + 1
            lex.linesSinceDocstring++
        };

        __ = (ws | newline)*;

        # Comments
        line_comment = ('#'|'//') [^\n]*;
        multiline_comment = '/*' (newline | any)* :>> '*/';

        # Symbols are sent to the parser as-is.
        symbol = [\*=<>\(\)\{\},;:\[\]];

        # String literals.
        literal
            = ('"' ([^"\n\\] | '\\' any)* '"')
            | ("'" ([^'\n\\] | '\\' any)* "'")
            ;

        identifier = [a-zA-Z_] ([a-zA-Z0-9_] | '.' [a-zA-Z0-9_])*;

        integer = ('+' | '-')? digit+;
        hex_integer = '0x' xdigit+;

        # Doubles can optionally have decimals and/or exponents, which means
        # this pattern can also reduce to just `integer`. Fortunately, this
        # token has a lower precendence, so we don't need to use multiple
        # patterns here to avoid ambiguities.
        double = integer ('.' digit*)? ([Ee] integer)?;

        # The following keywords are reserved in different languages and are
        # disallowed as identifiers in the IDL.
        #
        # From: https://github.com/apache/thrift/blob/775671aea41ea55427dd78d7ce68e282cc9b8487/compiler/cpp/src/thriftl.ll#L266
        reservedKeyword =
            ( 'BEGIN'
            | 'END'
            | '__CLASS__'
            | '__DIR__'
            | '__FILE__'
            | '__FUNCTION__'
            | '__LINE__'
            | '__METHOD__'
            | '__NAMESPACE__'
            | 'abstract'
            | 'alias'
            | 'and'
            | 'args'
            | 'as'
            | 'assert'
            | 'begin'
            | 'break'
            | 'case'
            | 'catch'
            | 'class'
            | 'clone'
            | 'continue'
            | 'declare'
            | 'def'
            | 'default'
            | 'del'
            | 'delete'
            | 'do'
            | 'dynamic'
            | 'elif'
            | 'else'
            | 'elseif'
            | 'elsif'
            | 'end'
            | 'enddeclare'
            | 'endfor'
            | 'endforeach'
            | 'endif'
            | 'endswitch'
            | 'endwhile'
            | 'ensure'
            | 'except'
            | 'exec'
            | 'finally'
            | 'float'
            | 'for'
            | 'foreach'
            | 'from'
            | 'function'
            | 'global'
            | 'goto'
            | 'if'
            | 'implements'
            | 'import'
            | 'in'
            | 'inline'
            | 'instanceof'
            | 'interface'
            | 'is'
            | 'lambda'
            | 'module'
            | 'native'
            | 'new'
            | 'next'
            | 'nil'
            | 'not'
            | 'or'
            | 'package'
            | 'pass'
            | 'public'
            | 'print'
            | 'private'
            | 'protected'
            | 'raise'
            | 'redo'
            | 'rescue'
            | 'retry'
            | 'register'
            | 'return'
            | 'self'
            | 'sizeof'
            | 'static'
            | 'super'
            | 'switch'
            | 'synchronized'
            | 'then'
            | 'this'
            | 'throw'
            | 'transient'
            | 'try'
            | 'undef'
            | 'unless'
            | 'unsigned'
            | 'until'
            | 'use'
            | 'var'
            | 'virtual'
            | 'volatile'
            | 'when'
            | 'while'
            | 'with'
            | 'xor'
            | 'yield'
            ) @{ reservedKeyword = string(lex.data[lex.ts:lex.te]) }
            ;

        main := |*
            # A note about the scanner:
            #
            # Ragel will usually generate a scanner that will process all
            # available input in a single call. For goyacc, we want to advance
            # only to the next symbol and return that and any associated
            # information.
            #
            # So we use the special 'fbreak' statement available in action
            # blocks that consumes the token, saves the state, and breaks out
            # of the scanner. This allows the next call to the function to
            # pick up where the scanner left off.
            #
            # Because of this, we save all state for the scanner on the lexer
            # object.

            # Keywords
            'include'     __ => { tok =       INCLUDE; fbreak; };
            'cpp_include' __ => { tok =   CPP_INCLUDE; fbreak; };
            'namespace'   __ => { tok =     NAMESPACE; fbreak; };
            'void'        __ => { tok =          VOID; fbreak; };
            'bool'        __ => { tok =          BOOL; fbreak; };
            'byte'        __ => { tok =          BYTE; fbreak; };
            'i8'          __ => { tok =            I8; fbreak; };
            'i16'         __ => { tok =           I16; fbreak; };
            'i32'         __ => { tok =           I32; fbreak; };
            'i64'         __ => { tok =           I64; fbreak; };
            'double'      __ => { tok =        DOUBLE; fbreak; };
            'string'      __ => { tok =        STRING; fbreak; };
            'binary'      __ => { tok =        BINARY; fbreak; };
            'map'         __ => { tok =           MAP; fbreak; };
            'list'        __ => { tok =          LIST; fbreak; };
            'set'         __ => { tok =           SET; fbreak; };
            'oneway'      __ => { tok =        ONEWAY; fbreak; };
            'typedef'     __ => { tok =       TYPEDEF; fbreak; };
            'struct'      __ => { tok =        STRUCT; fbreak; };
            'union'       __ => { tok =         UNION; fbreak; };
            'exception'   __ => { tok =     EXCEPTION; fbreak; };
            'extends'     __ => { tok =       EXTENDS; fbreak; };
            'throws'      __ => { tok =        THROWS; fbreak; };
            'service'     __ => { tok =       SERVICE; fbreak; };
            'enum'        __ => { tok =          ENUM; fbreak; };
            'const'       __ => { tok =         CONST; fbreak; };
            'required'    __ => { tok =      REQUIRED; fbreak; };
            'optional'    __ => { tok =      OPTIONAL; fbreak; };
            'true'        __ => { tok =          TRUE; fbreak; };
            'false'       __ => { tok =         FALSE; fbreak; };

            symbol => {
                tok = int(lex.data[lex.ts])
                fbreak;
            };

            # Ignore comments and whitespace
            ws;
            newline;
            docstring;
            line_comment;
            multiline_comment;

            (integer | hex_integer) => {
                str := string(lex.data[lex.ts:lex.te])
                base := 10
                if len(str) > 2 && str[0:2] == "0x" {
                    // Hex constant
                    base = 16
                }

                if i64, err := strconv.ParseInt(str, base, 64); err != nil {
                    lex.AppendError(err)
                } else {
                    out.i64 = i64
                    tok = INTCONSTANT
                }
                fbreak;
            };

            double => {
                str := string(lex.data[lex.ts:lex.te])
                if dub, err := strconv.ParseFloat(str, 64); err != nil {
                    lex.AppendError(err)
                } else {
                    out.dub = dub
                    tok = DUBCONSTANT
                }
                fbreak;
            };

            literal => {
                bs := lex.data[lex.ts:lex.te]

                var str string
                var err error
                if len(bs) > 0 && bs[0] == '\'' {
                    str, err = UnquoteSingleQuoted(bs)
                } else {
                    str, err = UnquoteDoubleQuoted(bs)
                }

                if err != nil {
                    lex.AppendError(err)
                } else {
                    out.str = str
                    tok = LITERAL
                }

                fbreak;
            };

            reservedKeyword __ => {
                lex.AppendError(fmt.Errorf("%q is a reserved keyword", reservedKeyword))
                fbreak;
            };

            identifier => {
                out.str = string(lex.data[lex.ts:lex.te])
                tok = IDENTIFIER
                fbreak;
            };
        *|;

        write exec;

    }%%

    if lex.cs == thrift_error {
        lex.Error("unknown token")
    }
    return tok
}

func (lex *lexer) Error(e string) {
    lex.AppendError(errors.New(e))
}

func (lex *lexer) AppendError(err error)  {
    lex.parseFailed = true
    lex.errors = append(lex.errors, ParseError{Pos: lex.Pos(), Err: err})
}

func (lex* lexer) Pos() ast.Position {
    return ast.Position{Line: lex.line, Column: lex.ts - lex.lineStart + 1}
}

func (lex* lexer) RecordPosition(n ast.Node, pos ast.Position) {
    lex.nodePositions[n] = pos
}

func (lex *lexer) LastDocstring() string {
    // If we've had more than one line since we recorded
    // the docstring, ignore it.
    if lex.linesSinceDocstring > 1 {
        return ""
    }

    s := lex.lastDocstring
    lex.lastDocstring = ""
    lex.linesSinceDocstring = 0
    return s
}
