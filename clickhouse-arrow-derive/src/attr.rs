// This module handles parsing of `#[clickhouse_arrow(...)]` attributes. The entrypoints
// are `attr::Container::from_ast`, `attr::Variant::from_ast`, and
// `attr::Field::from_ast`. Each returns an instance of the corresponding
// struct. Note that none of them return a Result. Unrecognized, malformed, or
// duplicated attributes result in a span_err but otherwise are ignored. The
// user will see errors simultaneously for all bad attributes in the crate
// rather than just the first.

use proc_macro2::{Span, TokenStream, TokenTree};
use quote::ToTokens;
use syn::Ident;
use syn::parse::{self, Parse, ParseStream};

use crate::case::RenameRule;
use crate::ctxt::Ctxt;
use crate::respan::respan;
use crate::symbol::*;

struct Attr<'c, T> {
    cx:     &'c Ctxt,
    name:   Symbol,
    tokens: TokenStream,
    value:  Option<T>,
}

impl<'c, T> Attr<'c, T> {
    fn none(cx: &'c Ctxt, name: Symbol) -> Self {
        Attr { cx, name, tokens: TokenStream::new(), value: None }
    }

    fn set<A: ToTokens>(&mut self, obj: A, value: T) {
        let tokens = obj.into_token_stream();

        if self.value.is_some() {
            self.cx.error_spanned_by(
                tokens,
                format!("duplicate clickhouse_arrow attribute `{}`", self.name),
            );
        } else {
            self.tokens = tokens;
            self.value = Some(value);
        }
    }

    fn set_opt<A: ToTokens>(&mut self, obj: A, value: Option<T>) {
        if let Some(value) = value {
            self.set(obj, value);
        }
    }

    fn set_if_none(&mut self, value: T) {
        if self.value.is_none() {
            self.value = Some(value);
        }
    }

    fn get(self) -> Option<T> { self.value }
}

struct BoolAttr<'c>(Attr<'c, ()>);

impl<'c> BoolAttr<'c> {
    fn none(cx: &'c Ctxt, name: Symbol) -> Self { BoolAttr(Attr::none(cx, name)) }

    fn set_true<A: ToTokens>(&mut self, obj: A) { self.0.set(obj, ()); }

    fn get(&self) -> bool { self.0.value.is_some() }
}

pub struct Name {
    name:    String,
    renamed: bool,
}

fn unraw(ident: &Ident) -> String { ident.to_string().trim_start_matches("r#").to_owned() }

impl Name {
    fn from_attrs(source_name: String, rename: Attr<String>) -> Name {
        let rename = rename.get();
        Name { renamed: rename.is_some(), name: rename.unwrap_or_else(|| source_name.clone()) }
    }

    pub fn name(&self) -> String { self.name.clone() }
}

pub struct Container {
    deny_unknown_fields: bool,
    default:             Default,
    rename_all_rule:     RenameRule,
    bound:               Option<Vec<syn::WherePredicate>>,
    type_from:           Option<syn::Type>,
    type_try_from:       Option<syn::Type>,
    type_into:           Option<syn::Type>,
    // #[clickhouse_arrow(schema = "get_schema")]
    schema:              Option<syn::ExprPath>,
    is_packed:           bool,
}

impl Container {
    pub fn from_ast(cx: &Ctxt, item: &syn::DeriveInput) -> Self {
        let mut rename = Attr::none(cx, RENAME);
        let mut deny_unknown_fields = BoolAttr::none(cx, DENY_UNKNOWN_FIELDS);
        let mut default = Attr::none(cx, DEFAULT);
        let mut rename_all_rule = Attr::none(cx, RENAME_ALL);
        let mut bound = Attr::none(cx, BOUND);
        let mut type_from = Attr::none(cx, FROM);
        let mut type_try_from = Attr::none(cx, TRY_FROM);
        let mut type_into = Attr::none(cx, INTO);
        let mut schema = Attr::none(cx, SCHEMA);

        for attr in &item.attrs {
            if attr.path().is_ident("clickhouse_arrow") {
                let _ = attr.parse_nested_meta(|meta| {
                    match meta.path.get_ident() {
                        Some(ident) if ident == RENAME => {
                            if let Ok(expr) = meta.value()
                                && let Ok(s) = expr.parse::<syn::LitStr>()
                            {
                                rename.set(&meta.path, s.value());
                            }
                        }
                        Some(ident) if ident == RENAME_ALL => {
                            if let Ok(expr) = meta.value()
                                && let Ok(s) = expr.parse::<syn::LitStr>()
                            {
                                match RenameRule::from_str(&s.value()) {
                                    Ok(rule) => rename_all_rule.set(&meta.path, rule),
                                    Err(err) => cx.error_spanned_by(s, err),
                                }
                            }
                        }
                        Some(ident) if ident == DENY_UNKNOWN_FIELDS => {
                            deny_unknown_fields.set_true(&meta.path);
                        }
                        Some(ident) if ident == DEFAULT => {
                            if meta.input.peek(syn::Token![=]) {
                                if let Ok(expr) = meta.value()
                                    && let Ok(path) = expr.parse::<syn::ExprPath>()
                                {
                                    match &item.data {
                                        syn::Data::Struct(syn::DataStruct { fields, .. }) => {
                                            match fields {
                                                syn::Fields::Named(_) => {
                                                    default.set(&meta.path, Default::Path(path))
                                                }
                                                _ => cx.error_spanned_by(
                                                    fields,
                                                    "#[clickhouse_arrow(default = \"...\")] can \
                                                     only be used on structs with named fields",
                                                ),
                                            }
                                        }
                                        _ => cx.error_spanned_by(
                                            item,
                                            "#[clickhouse_arrow(default = \"...\")] can only be \
                                             used on structs with named fields",
                                        ),
                                    }
                                }
                            } else {
                                match &item.data {
                                    syn::Data::Struct(syn::DataStruct { fields, .. }) => {
                                        match fields {
                                            syn::Fields::Named(_) => {
                                                default.set(&meta.path, Default::Default)
                                            }
                                            _ => cx.error_spanned_by(
                                                fields,
                                                "#[clickhouse_arrow(default)] can only be used on \
                                                 structs with named fields",
                                            ),
                                        }
                                    }
                                    _ => cx.error_spanned_by(
                                        item,
                                        "#[clickhouse_arrow(default)] can only be used on structs \
                                         with named fields",
                                    ),
                                }
                            }
                        }
                        Some(ident) if ident == BOUND => {
                            if let Ok(buffer) = meta.value() {
                                let expr = buffer.parse::<syn::Expr>()?;
                                if let Ok(predicates) =
                                    parse_lit_into_where(cx, BOUND, BOUND, &expr)
                                {
                                    bound.set(&meta.path, predicates);
                                }
                            }
                        }
                        Some(ident) if ident == FROM => {
                            if let Ok(expr) = meta.value()
                                && let Ok(ty) = expr.parse::<syn::Type>()
                            {
                                type_from.set_opt(&meta.path, Some(ty));
                            }
                        }
                        Some(ident) if ident == TRY_FROM => {
                            if let Ok(expr) = meta.value()
                                && let Ok(ty) = expr.parse::<syn::Type>()
                            {
                                type_try_from.set_opt(&meta.path, Some(ty));
                            }
                        }
                        Some(ident) if ident == SCHEMA => {
                            if meta.input.peek(syn::Token![=]) {
                                if let Ok(expr) = meta.value()
                                    && let Ok(path) = expr.parse::<syn::ExprPath>()
                                {
                                    schema.set(&meta.path, path);
                                } else {
                                    cx.error_spanned_by(
                                        meta.path,
                                        "expected function path for `schema` attribute",
                                    );
                                }
                            } else {
                                cx.error_spanned_by(
                                    meta.path,
                                    "schema attribute requires a value, e.g., `schema = \
                                     \"get_schema\"`",
                                );
                            }
                        }
                        Some(ident) if ident == INTO => {
                            if let Ok(expr) = meta.value()
                                && let Ok(ty) = expr.parse::<syn::Type>()
                            {
                                type_into.set_opt(&meta.path, Some(ty));
                            }
                        }
                        _ => {
                            let path =
                                meta.path.clone().into_token_stream().to_string().replace(' ', "");
                            cx.error_spanned_by(
                                meta.path,
                                format!("unknown clickhouse_arrow container attribute `{}`", path),
                            );
                        }
                    }
                    Ok(())
                });
            }
        }

        let mut is_packed = false;
        for attr in &item.attrs {
            if attr.path().is_ident("repr") {
                let _ = attr.parse_args_with(|input: ParseStream| {
                    while !input.is_empty() {
                        let token = input.parse::<TokenTree>()?;
                        if let TokenTree::Ident(ident) = token {
                            is_packed |= ident == "packed";
                        }
                    }
                    Ok(())
                });
            }
        }

        Container {
            deny_unknown_fields: deny_unknown_fields.get(),
            default: default.get().unwrap_or(Default::None),
            rename_all_rule: rename_all_rule.get().unwrap_or(RenameRule::None),
            bound: bound.get(),
            type_from: type_from.get(),
            type_try_from: type_try_from.get(),
            type_into: type_into.get(),
            schema: schema.get(),
            is_packed,
        }
    }

    pub fn rename_all_rule(&self) -> &RenameRule { &self.rename_all_rule }

    pub fn deny_unknown_fields(&self) -> bool { self.deny_unknown_fields }

    pub fn default(&self) -> &Default { &self.default }

    pub fn bound(&self) -> Option<&[syn::WherePredicate]> {
        self.bound.as_ref().map(|vec| &vec[..])
    }

    pub fn type_from(&self) -> Option<&syn::Type> { self.type_from.as_ref() }

    pub fn type_try_from(&self) -> Option<&syn::Type> { self.type_try_from.as_ref() }

    pub fn type_into(&self) -> Option<&syn::Type> { self.type_into.as_ref() }

    pub fn schema(&self) -> Option<&syn::ExprPath> { self.schema.as_ref() }

    pub fn is_packed(&self) -> bool { self.is_packed }
}

pub struct Field {
    name:               Name,
    skip_serializing:   bool,
    skip_deserializing: bool,
    default:            Default,
    serialize_with:     Option<syn::ExprPath>,
    deserialize_with:   Option<syn::ExprPath>,
    bound:              Option<Vec<syn::WherePredicate>>,
    nested:             bool,
    flatten:            bool,
}

#[allow(clippy::enum_variant_names)]
pub enum Default {
    None,
    Default,
    Path(syn::ExprPath),
}

impl Field {
    pub fn from_ast(
        cx: &Ctxt,
        index: usize,
        field: &syn::Field,
        container_default: &Default,
    ) -> Self {
        let mut rename = Attr::none(cx, RENAME);
        let mut nested = BoolAttr::none(cx, NESTED);
        let mut skip_serializing = BoolAttr::none(cx, SKIP_SERIALIZING);
        let mut skip_deserializing = BoolAttr::none(cx, SKIP_DESERIALIZING);
        let mut flatten = BoolAttr::none(cx, FLATTEN);
        let mut default = Attr::none(cx, DEFAULT);
        let mut serialize_with = Attr::none(cx, SERIALIZE_WITH);
        let mut deserialize_with = Attr::none(cx, DESERIALIZE_WITH);
        let mut bound = Attr::none(cx, BOUND);

        let ident = match &field.ident {
            Some(ident) => unraw(ident),
            None => index.to_string(),
        };

        for attr in &field.attrs {
            if attr.path().is_ident("clickhouse_arrow") {
                let _ = attr.parse_nested_meta(|meta| {
                    match meta.path.get_ident() {
                        Some(ident) if ident == RENAME => {
                            if let Ok(expr) = meta.value()
                                && let Ok(s) = expr.parse::<syn::LitStr>()
                            {
                                rename.set(&meta.path, s.value());
                            }
                        }
                        Some(ident) if ident == DEFAULT => {
                            if meta.input.peek(syn::Token![=]) {
                                if let Ok(expr) = meta.value()
                                    && let Ok(path) = expr.parse::<syn::ExprPath>()
                                {
                                    default.set(&meta.path, Default::Path(path));
                                }
                            } else {
                                default.set(&meta.path, Default::Default);
                            }
                        }
                        Some(ident) if ident == SKIP_SERIALIZING => {
                            skip_serializing.set_true(&meta.path);
                        }
                        Some(ident) if ident == NESTED => {
                            nested.set_true(&meta.path);
                        }
                        Some(ident) if ident == FLATTEN => {
                            flatten.set_true(&meta.path);
                        }
                        Some(ident) if ident == SKIP_DESERIALIZING => {
                            skip_deserializing.set_true(&meta.path);
                        }
                        Some(ident) if ident == SKIP => {
                            skip_serializing.set_true(&meta.path);
                            skip_deserializing.set_true(&meta.path);
                        }
                        Some(ident) if ident == SERIALIZE_WITH => {
                            if let Ok(expr) = meta.value()
                                && let Ok(path) = expr.parse::<syn::ExprPath>()
                            {
                                serialize_with.set(&meta.path, path);
                            }
                        }
                        Some(ident) if ident == DESERIALIZE_WITH => {
                            if let Ok(expr) = meta.value()
                                && let Ok(path) = expr.parse::<syn::ExprPath>()
                            {
                                deserialize_with.set(&meta.path, path);
                            }
                        }
                        Some(ident) if ident == WITH => {
                            if let Ok(expr) = meta.value()
                                && let Ok(path) = expr.parse::<syn::ExprPath>()
                            {
                                let mut ser_path = path.clone();
                                ser_path
                                    .path
                                    .segments
                                    .push(Ident::new("to_sql", Span::call_site()).into());
                                serialize_with.set(&meta.path, ser_path);
                                let mut de_path = path;
                                de_path
                                    .path
                                    .segments
                                    .push(Ident::new("from_sql", Span::call_site()).into());
                                deserialize_with.set(&meta.path, de_path);
                            }
                        }
                        Some(ident) if ident == BOUND => {
                            if let Ok(buffer) = meta.value() {
                                let expr = buffer.parse::<syn::Expr>()?;
                                if let Ok(predicates) =
                                    parse_lit_into_where(cx, BOUND, BOUND, &expr)
                                {
                                    bound.set(&meta.path, predicates);
                                }
                            }
                        }
                        _ => {
                            let path =
                                meta.path.clone().into_token_stream().to_string().replace(' ', "");
                            cx.error_spanned_by(
                                meta.path,
                                format!("unknown clickhouse_arrow field attribute `{}`", path),
                            );
                        }
                    }
                    Ok(())
                });
            }
        }

        if let Default::None = *container_default
            && skip_deserializing.0.value.is_some()
        {
            default.set_if_none(Default::Default);
        }

        Field {
            name:               Name::from_attrs(ident, rename),
            skip_serializing:   skip_serializing.get(),
            skip_deserializing: skip_deserializing.get(),
            default:            default.get().unwrap_or(Default::None),
            serialize_with:     serialize_with.get(),
            deserialize_with:   deserialize_with.get(),
            bound:              bound.get(),
            nested:             nested.get(),
            flatten:            flatten.get(),
        }
    }

    pub fn name(&self) -> &Name { &self.name }

    pub fn rename_by_rules(&mut self, rules: &RenameRule) {
        if !self.name.renamed {
            self.name.name = rules.apply_to_field(&self.name.name);
        }
    }

    pub fn flatten(&self) -> bool { self.flatten }

    pub fn nested(&self) -> bool { self.nested }

    pub fn skip_serializing(&self) -> bool { self.skip_serializing }

    pub fn skip_deserializing(&self) -> bool { self.skip_deserializing }

    pub fn default(&self) -> &Default { &self.default }

    pub fn serialize_with(&self) -> Option<&syn::ExprPath> { self.serialize_with.as_ref() }

    pub fn deserialize_with(&self) -> Option<&syn::ExprPath> { self.deserialize_with.as_ref() }

    pub fn bound(&self) -> Option<&[syn::WherePredicate]> {
        self.bound.as_ref().map(|vec| &vec[..])
    }
}

#[expect(unused)]
pub fn get_clickhouse_native_meta_items(cx: &Ctxt, attr: &syn::Attribute) -> Result<(), ()> {
    if !attr.path().is_ident("clickhouse_arrow") {
        return Ok(());
    }

    attr.parse_nested_meta(|meta| {
        cx.error_spanned_by(meta.path, "unexpected literal in clickhouse_arrow attribute");
        Ok(())
    })
    .map_err(|err| {
        cx.syn_error(err);
    })?;
    Ok(())
}

fn get_lit_str(cx: &Ctxt, attr_name: Symbol, expr: &syn::Expr) -> Result<syn::LitStr, ()> {
    if let syn::Expr::Lit(syn::ExprLit { lit: syn::Lit::Str(lit), .. }) = expr {
        Ok(lit.clone())
    } else {
        cx.error_spanned_by(
            expr,
            format!(
                "expected clickhouse_arrow {} attribute to be a string: `{} = \"...\"`",
                attr_name, attr_name
            ),
        );
        Err(())
    }
}

fn get_lit_str2(
    cx: &Ctxt,
    attr_name: Symbol,
    meta_item_name: Symbol,
    expr: &syn::Expr,
) -> Result<syn::LitStr, ()> {
    if let syn::Expr::Lit(syn::ExprLit { lit: syn::Lit::Str(lit), .. }) = expr {
        Ok(lit.clone())
    } else {
        cx.error_spanned_by(
            expr,
            format!(
                "expected clickhouse_arrow {} attribute to be a string: `{} = \"...\"`",
                attr_name, meta_item_name
            ),
        );
        Err(())
    }
}

#[expect(unused)]
fn parse_lit_into_expr_path(
    cx: &Ctxt,
    attr_name: Symbol,
    expr: &syn::Expr,
) -> Result<syn::ExprPath, ()> {
    let string = get_lit_str(cx, attr_name, expr)?;
    parse_lit_str(&string).map_err(|_| {
        cx.error_spanned_by(expr, format!("failed to parse path: {:?}", string.value()))
    })
}

fn parse_lit_into_where(
    cx: &Ctxt,
    attr_name: Symbol,
    meta_item_name: Symbol,
    expr: &syn::Expr,
) -> Result<Vec<syn::WherePredicate>, ()> {
    let string = get_lit_str2(cx, attr_name, meta_item_name, expr)?;
    if string.value().is_empty() {
        return Ok(Vec::new());
    }

    let where_string = syn::LitStr::new(&format!("where {}", string.value()), string.span());

    parse_lit_str::<syn::WhereClause>(&where_string)
        .map(|wh| wh.predicates.into_iter().collect())
        .map_err(|err| cx.error_spanned_by(expr, err))
}

#[expect(unused)]
fn parse_lit_into_ty(cx: &Ctxt, attr_name: Symbol, expr: &syn::Expr) -> Result<syn::Type, ()> {
    let string = get_lit_str(cx, attr_name, expr)?;

    parse_lit_str(&string).map_err(|_| {
        cx.error_spanned_by(
            expr,
            format!("failed to parse type: {} = {:?}", attr_name, string.value()),
        )
    })
}

fn parse_lit_str<T>(s: &syn::LitStr) -> parse::Result<T>
where
    T: Parse,
{
    let tokens = spanned_tokens(s)?;
    syn::parse2(tokens)
}

fn spanned_tokens(s: &syn::LitStr) -> parse::Result<TokenStream> {
    let stream = syn::parse_str(&s.value())?;
    Ok(respan(stream, s.span()))
}
