use std::collections::HashSet;

use syn::punctuated::Pair;

use crate::ast::Container;
use crate::attr;
use crate::internal::ungroup;

pub fn without_defaults(generics: &syn::Generics) -> syn::Generics {
    syn::Generics {
        params: generics
            .params
            .iter()
            .map(|param| match param {
                syn::GenericParam::Type(param) => syn::GenericParam::Type(syn::TypeParam {
                    eq_token: None,
                    default: None,
                    ..param.clone()
                }),
                _ => param.clone(),
            })
            .collect(),
        ..generics.clone()
    }
}

pub fn with_where_predicates(
    generics: &syn::Generics,
    predicates: &[syn::WherePredicate],
) -> syn::Generics {
    let mut generics = generics.clone();
    generics.make_where_clause().predicates.extend(predicates.iter().cloned());
    generics
}

pub fn with_where_predicates_from_fields(
    cont: &Container,
    generics: &syn::Generics,
    from_field: fn(&attr::Field) -> Option<&[syn::WherePredicate]>,
) -> syn::Generics {
    let predicates = cont
        .data
        .iter()
        .filter_map(|field| from_field(&field.attrs))
        .flat_map(|predicates| predicates.to_vec());

    let mut generics = generics.clone();
    generics.make_where_clause().predicates.extend(predicates);
    generics
}

pub fn with_bound(
    cont: &Container,
    generics: &syn::Generics,
    filter: fn(&attr::Field) -> bool,
    bound: &[&syn::Path],
) -> syn::Generics {
    struct FindTyParams<'ast> {
        all_type_params:       HashSet<syn::Ident>,
        relevant_type_params:  HashSet<syn::Ident>,
        associated_type_usage: Vec<&'ast syn::TypePath>,
    }

    impl<'ast> FindTyParams<'ast> {
        fn visit_field(&mut self, field: &'ast syn::Field) {
            if let syn::Type::Path(ty) = ungroup(&field.ty)
                && let Some(Pair::Punctuated(t, _)) = ty.path.segments.pairs().next()
                && self.all_type_params.contains(&t.ident)
            {
                self.associated_type_usage.push(ty);
            }
            self.visit_type(&field.ty);
        }

        fn visit_path(&mut self, path: &'ast syn::Path) {
            if let Some(seg) = path.segments.last()
                && seg.ident == "PhantomData"
            {
                return;
            }
            if path.leading_colon.is_none() && path.segments.len() == 1 {
                let id = &path.segments[0].ident;
                if self.all_type_params.contains(id) {
                    self.relevant_type_params.insert(id.clone());
                }
            }
            for segment in &path.segments {
                self.visit_path_segment(segment);
            }
        }

        fn visit_type(&mut self, ty: &'ast syn::Type) {
            match ty {
                syn::Type::Array(ty) => self.visit_type(&ty.elem),
                syn::Type::BareFn(ty) => {
                    for arg in &ty.inputs {
                        self.visit_type(&arg.ty);
                    }
                    self.visit_return_type(&ty.output);
                }
                syn::Type::Group(ty) => self.visit_type(&ty.elem),
                syn::Type::ImplTrait(ty) => {
                    for bound in &ty.bounds {
                        self.visit_type_param_bound(bound);
                    }
                }
                syn::Type::Macro(ty) => self.visit_macro(&ty.mac),
                syn::Type::Paren(ty) => self.visit_type(&ty.elem),
                syn::Type::Path(ty) => {
                    if let Some(qself) = &ty.qself {
                        self.visit_type(&qself.ty);
                    }
                    self.visit_path(&ty.path);
                }
                syn::Type::Ptr(ty) => self.visit_type(&ty.elem),
                syn::Type::Reference(ty) => self.visit_type(&ty.elem),
                syn::Type::Slice(ty) => self.visit_type(&ty.elem),
                syn::Type::TraitObject(ty) => {
                    for bound in &ty.bounds {
                        self.visit_type_param_bound(bound);
                    }
                }
                syn::Type::Tuple(ty) => {
                    for elem in &ty.elems {
                        self.visit_type(elem);
                    }
                }
                syn::Type::Infer(_) | syn::Type::Never(_) | syn::Type::Verbatim(_) => {}
                _ => {}
            }
        }

        fn visit_path_segment(&mut self, segment: &'ast syn::PathSegment) {
            self.visit_path_arguments(&segment.arguments);
        }

        fn visit_path_arguments(&mut self, arguments: &'ast syn::PathArguments) {
            match arguments {
                syn::PathArguments::None => {}
                syn::PathArguments::AngleBracketed(arguments) => {
                    for arg in &arguments.args {
                        match arg {
                            syn::GenericArgument::Type(arg) => self.visit_type(arg),
                            syn::GenericArgument::AssocType(arg) => self.visit_type(&arg.ty),
                            syn::GenericArgument::Lifetime(_)
                            | syn::GenericArgument::Constraint(_)
                            | syn::GenericArgument::Const(_) => {}
                            _ => {} // Wildcard for exhaustiveness
                        }
                    }
                }
                syn::PathArguments::Parenthesized(arguments) => {
                    for argument in &arguments.inputs {
                        self.visit_type(argument);
                    }
                    self.visit_return_type(&arguments.output);
                }
            }
        }

        fn visit_return_type(&mut self, return_type: &'ast syn::ReturnType) {
            match return_type {
                syn::ReturnType::Default => {}
                syn::ReturnType::Type(_, output) => self.visit_type(output),
            }
        }

        fn visit_type_param_bound(&mut self, bound: &'ast syn::TypeParamBound) {
            match bound {
                syn::TypeParamBound::Trait(bound) => self.visit_path(&bound.path),
                syn::TypeParamBound::Lifetime(_) => {}
                syn::TypeParamBound::Verbatim(_) => {}
                _ => {} // Wildcard for exhaustiveness
            }
        }

        fn visit_macro(&mut self, _mac: &'ast syn::Macro) {}
    }

    let all_type_params = generics.type_params().map(|param| param.ident.clone()).collect();

    let mut visitor = FindTyParams {
        all_type_params,
        relevant_type_params: HashSet::new(),
        associated_type_usage: Vec::new(),
    };
    for field in cont.data.iter().filter(|field| filter(&field.attrs)) {
        visitor.visit_field(field.original);
    }

    let relevant_type_params = visitor.relevant_type_params;
    let associated_type_usage = visitor.associated_type_usage;
    let new_predicates = generics
        .type_params()
        .map(|param| param.ident.clone())
        .filter(|id| relevant_type_params.contains(id))
        .map(|id| syn::TypePath { qself: None, path: id.into() })
        .chain(associated_type_usage.into_iter().cloned())
        .map(|bounded_ty| {
            syn::WherePredicate::Type(syn::PredicateType {
                lifetimes:   None,
                bounded_ty:  syn::Type::Path(bounded_ty),
                colon_token: <syn::token::Colon>::default(),
                bounds:      bound
                    .iter()
                    .map(|bound| {
                        syn::TypeParamBound::Trait(syn::TraitBound {
                            paren_token: None,
                            modifier:    syn::TraitBoundModifier::None,
                            lifetimes:   None,
                            path:        (*bound).clone(),
                        })
                    })
                    .collect(),
            })
        });

    let mut generics = generics.clone();
    generics.make_where_clause().predicates.extend(new_predicates);
    generics
}
