%{
package internal

import "go.uber.org/thriftrw/ast"
%}

%union {
    // Used to record line and column numbers when position is required.
    line int
    pos ast.Position

    docstring string

    // Holds the final AST for the file.
    prog *ast.Program

    // Other intermediate variables:

    bul bool
    str string
    i64 int64
    dub float64

    fieldType ast.Type
    structType ast.StructureType
    baseTypeID ast.BaseTypeID
    fieldIdentifier fieldIdentifier
    fieldRequired ast.Requiredness

    field *ast.Field
    fields []*ast.Field

    header ast.Header
    headers []ast.Header

    function *ast.Function
    functions []*ast.Function

    enumItem *ast.EnumItem
    enumItems []*ast.EnumItem

    definition ast.Definition
    definitions []ast.Definition

    typeAnnotations []*ast.Annotation

    constantValue ast.ConstantValue
    constantValues []ast.ConstantValue
    constantMapItems []ast.ConstantMapItem
}

%token <str> IDENTIFIER
%token <str> LITERAL
%token <i64> INTCONSTANT
%token <dub> DUBCONSTANT

// Reserved keywords
%token NAMESPACE INCLUDE CPP_INCLUDE
%token VOID BOOL BYTE I8 I16 I32 I64 DOUBLE STRING BINARY MAP LIST SET
%token ONEWAY TYPEDEF STRUCT UNION EXCEPTION EXTENDS THROWS SERVICE ENUM CONST
%token REQUIRED OPTIONAL TRUE FALSE

%type <pos> pos
%type <docstring> docstring
%type <prog> program
%type <fieldType> type
%type <baseTypeID> base_type_name
%type <fieldIdentifier> field_identifier
%type <fieldRequired> field_required
%type <structType> struct_type

%type <field> field
%type <fields> fields

%type <header> header
%type <headers> headers

%type <function> function
%type <functions> functions

%type <enumItem> enum_item
%type <enumItems> enum_items

%type <definition> definition
%type <definitions> definitions

%type <constantValue> const_value
%type <constantValues> const_list_items
%type <constantMapItems> const_map_items

%type <typeAnnotations> type_annotation_list type_annotations

%%

program
    : headers definitions
        {
            $$ = &ast.Program{Headers: $1, Definitions: $2}
            yylex.(*lexer).program = $$
            return 0
        }
    ;

/***************************************************************************
 Headers
 ***************************************************************************/

headers
    : /* no headers */     { $$ = nil }
    | headers header     { $$ = append($1, $2) }
    ;

header
    : pos INCLUDE LITERAL
        {
            $$ = &ast.Include{
                Path: $3,
                Line: $1.Line,
                Column: $1.Column,
            }
        }
    | pos INCLUDE IDENTIFIER LITERAL
        {
            $$ = &ast.Include{
                Name: $3,
                Path: $4,
                Line: $1.Line,
                Column: $1.Column,
            }
        }
    | pos CPP_INCLUDE LITERAL
        {
            $$ = &ast.CppInclude{
                Path: $3,
                Line: $1.Line,
                Column: $1.Column,
            }
        }
    | pos NAMESPACE '*' IDENTIFIER
        {
            $$ = &ast.Namespace{
                Scope: "*",
                Name: $4,
                Line: $1.Line,
                Column: $1.Column,
            }
        }
    | pos NAMESPACE IDENTIFIER IDENTIFIER
        {
            $$ = &ast.Namespace{
                Scope: $3,
                Name: $4,
                Line: $1.Line,
                Column: $1.Column,
            }
        }
    ;

/***************************************************************************
 Definitions
 ***************************************************************************/

definitions
    : /* nothing */ { $$ = nil }
    | definitions definition optional_sep { $$ = append($1, $2) }
    ;


definition
    /* constants */
    : pos docstring CONST type IDENTIFIER '=' const_value
        {
            $$ = &ast.Constant{
                Name: $5,
                Type: $4,
                Value: $7,
                Line: $1.Line,
                Column: $1.Column,
                Doc: ParseDocstring($2),
            }
        }
    /* types */
    | pos docstring TYPEDEF type IDENTIFIER type_annotations
        {
            $$ = &ast.Typedef{
                Name: $5,
                Type: $4,
                Annotations: $6,
                Line: $1.Line,
                Column: $1.Column,
                Doc: ParseDocstring($2),
            }
        }
    | pos docstring ENUM IDENTIFIER '{' enum_items '}' type_annotations
        {
            $$ = &ast.Enum{
                Name: $4,
                Items: $6,
                Annotations: $8,
                Line: $1.Line,
                Column: $1.Column,
                Doc: ParseDocstring($2),
            }
        }
    | pos docstring struct_type IDENTIFIER '{' fields '}' type_annotations
        {
            $$ = &ast.Struct{
                Name: $4,
                Type: $3,
                Fields: $6,
                Annotations: $8,
                Line: $1.Line,
                Column: $1.Column,
                Doc: ParseDocstring($2),
            }
        }
    /* services */
    | pos docstring SERVICE IDENTIFIER '{' functions '}' type_annotations
        {
            $$ = &ast.Service{
                Name: $4,
                Functions: $6,
                Annotations: $8,
                Line: $1.Line,
                Column: $1.Column,
                Doc: ParseDocstring($2),
            }
        }
    | pos docstring SERVICE IDENTIFIER EXTENDS pos IDENTIFIER '{' functions '}'
      type_annotations
        {
            parent := &ast.ServiceReference{
                Name: $7,
                Line: $6.Line,
                Column: $6.Column,
            }

            $$ = &ast.Service{
                Name: $4,
                Functions: $9,
                Parent: parent,
                Annotations: $11,
                Line: $1.Line,
                Column: $1.Column,
                Doc: ParseDocstring($2),
            }
        }
    ;

struct_type
    : STRUCT    { $$ =    ast.StructType }
    | UNION     { $$ =     ast.UnionType }
    | EXCEPTION { $$ = ast.ExceptionType }
    ;

enum_items
    : /* nothing */ { $$ = nil }
    | enum_items enum_item optional_sep { $$ = append($1, $2) }
    ;

enum_item
    : pos docstring IDENTIFIER type_annotations
        {
            $$ = &ast.EnumItem{
                Name: $3,
                Annotations: $4,
                Line: $1.Line,
                Column: $1.Column,
                Doc: ParseDocstring($2),
            }
        }
    | pos docstring IDENTIFIER '=' INTCONSTANT type_annotations
        {
            value := int($5)
            $$ = &ast.EnumItem{
                Name: $3,
                Value: &value,
                Annotations: $6,
                Line: $1.Line,
                Column: $1.Column,
                Doc: ParseDocstring($2),
            }
        }
    ;

fields
    : /* nothing */             { $$ = nil }
    | fields field optional_sep { $$ = append($1, $2) }
    ;

field
    : pos docstring field_identifier field_required type IDENTIFIER type_annotations
        {
            $$ = &ast.Field{
                ID: $3.ID,
                IDUnset: $3.Unset,
                Name: $6,
                Type: $5,
                Requiredness: $4,
                Annotations: $7,
                Line: $1.Line,
                Column: $1.Column,
                Doc: ParseDocstring($2),
            }
        }
    | pos docstring field_identifier field_required type IDENTIFIER '=' const_value type_annotations
        {
            $$ = &ast.Field{
                ID: $3.ID,
                IDUnset: $3.Unset,
                Name: $6,
                Type: $5,
                Requiredness: $4,
                Default: $8,
                Annotations: $9,
                Line: $1.Line,
                Column: $1.Column,
                Doc: ParseDocstring($2),
            }
        }
    ;

field_identifier
    : INTCONSTANT ':' { $$ = fieldIdentifier{ID: int($1)} }
    | /* na */        { $$ = fieldIdentifier{Unset: true} }
    ;

field_required
    : REQUIRED { $$ =    ast.Required }
    | OPTIONAL { $$ =    ast.Optional }
    | /* na */ { $$ = ast.Unspecified }
    ;

functions
    : /* nothing */ { $$ = nil }
    | functions function optional_sep { $$ = append($1, $2) }
    ;

function
    : docstring pos oneway function_type IDENTIFIER '(' fields ')' throws
      type_annotations
        {
            $$ = &ast.Function{
                Name: $5,
                Parameters: $7,
                ReturnType: $<fieldType>4,
                Exceptions: $<fields>9,
                OneWay: $<bul>3,
                Annotations: $10,
                Line: $2.Line,
                Column: $2.Column,
                Doc: ParseDocstring($1),
            }
        }
    ;

oneway
    : ONEWAY        { $<bul>$ = true }
    | /* nothing */ { $<bul>$ = false }
    ;

function_type
    : VOID { $<fieldType>$ = nil }
    | type { $<fieldType>$ = $1  }
    ;

throws
    : /* nothing */  { $<fields>$ = nil }
    | THROWS '(' fields ')' { $<fields>$ = $3 }
    ;

/***************************************************************************
 Types
 ***************************************************************************/

type
    : pos base_type_name type_annotations
        { $$ = ast.BaseType{ID: $2, Annotations: $3, Line: $1.Line, Column: $1.Column} }

    /* container types */
    | pos MAP '<' type ',' type '>' type_annotations
        { $$ = ast.MapType{KeyType: $4, ValueType: $6, Annotations: $8, Line: $1.Line, Column: $1.Column} }
    | pos LIST '<' type '>' type_annotations
        { $$ = ast.ListType{ValueType: $4, Annotations: $6, Line: $1.Line, Column: $1.Column} }
    | pos SET '<' type '>' type_annotations
        { $$ = ast.SetType{ValueType: $4, Annotations: $6, Line: $1.Line, Column: $1.Column} }

    /* type references ('pos' is last to avoid selection preference over 'base_type_name') */
    | IDENTIFIER pos
        { $$ = ast.TypeReference{Name: $1, Line: $2.Line, Column: $2.Column} }
    ;

base_type_name
    : BOOL    { $$ =   ast.BoolTypeID }
    | BYTE    { $$ =     ast.I8TypeID }
    | I8      { $$ =     ast.I8TypeID }
    | I16     { $$ =    ast.I16TypeID }
    | I32     { $$ =    ast.I32TypeID }
    | I64     { $$ =    ast.I64TypeID }
    | DOUBLE  { $$ = ast.DoubleTypeID }
    | STRING  { $$ = ast.StringTypeID }
    | BINARY  { $$ = ast.BinaryTypeID }
    ;

/***************************************************************************
 Constant values
 ***************************************************************************/
const_value
    : pos INTCONSTANT { $$ = ast.ConstantInteger($2); yylex.(*lexer).RecordPosition($$, $1) }
    | pos DUBCONSTANT { $$ = ast.ConstantDouble($2); yylex.(*lexer).RecordPosition($$, $1) }
    | pos TRUE        { $$ = ast.ConstantBoolean(true); yylex.(*lexer).RecordPosition($$, $1) }
    | pos FALSE       { $$ = ast.ConstantBoolean(false); yylex.(*lexer).RecordPosition($$, $1) }
    | pos LITERAL     { $$ = ast.ConstantString($2); yylex.(*lexer).RecordPosition($$, $1) }
    | pos IDENTIFIER  { $$ = ast.ConstantReference{Name: $2, Line: $1.Line, Column: $1.Column} }
    | pos '[' const_list_items ']' { $$ = ast.ConstantList{Items: $3, Line: $1.Line, Column: $1.Column} }
    | pos '{' const_map_items  '}' { $$ =  ast.ConstantMap{Items: $3, Line: $1.Line, Column: $1.Column} }
    ;

const_list_items
    : /* nothing */ { $$ = nil }
    | const_list_items const_value optional_sep
        { $$ = append($1, $2) }
    ;

const_map_items
    : /* nothing */ { $$ = nil }
    | const_map_items pos const_value ':' const_value optional_sep
        { $$ = append($1, ast.ConstantMapItem{Key: $3, Value: $5, Line: $2.Line, Column: $2.Column}) }
    ;

/***************************************************************************
 Type annotations
 ***************************************************************************/

type_annotations
    : /* nothing */         { $$ = nil }
    | '(' type_annotation_list ')' { $$ = $2 }
    ;

type_annotation_list
    : /* nothing */ { $$ = nil }
    | type_annotation_list pos IDENTIFIER '=' LITERAL optional_sep
        { $$ = append($1, &ast.Annotation{Name: $3, Value: $5, Line: $2.Line, Column: $2.Column}) }
    | type_annotation_list pos IDENTIFIER optional_sep
        { $$ = append($1, &ast.Annotation{Name: $3, Line: $2.Line, Column: $2.Column}) }
    ;

/***************************************************************************
 Other
 ***************************************************************************/

/* Grammar rules that need to record a line number at a specific token should
   include this somewhere. For example,

    foo : bar pos baz { x := $2 }

  $2 in the above example contains the line number right after 'bar' but before
  'baz'. This way, if 'baz' spans mulitple lines, we still get the line number
  for where the rule started rather than where it ends.
 */

pos
    : /* nothing */ { $$ = yylex.(*lexer).Pos() }
    ;

docstring
    : /* nothing */ { $$ = yylex.(*lexer).LastDocstring() }
    ;

optional_sep
    : ','
    | ';'
    | /* nothing */
    ;

// vim:set ft=yacc:
