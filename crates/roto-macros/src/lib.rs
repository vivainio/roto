//! `#[implements_operations]` for `impl Service for X` blocks of the generated service traits.
//!
//! The generated trait gives every operation a default method that answers `NotImplemented`;
//! a service implements the operations it supports by overriding methods. The attribute
//! records exactly the methods the block defines, as `roto_core::ImplementsOperations`, so
//! "supported" is the impl itself and cannot drift from it.
use proc_macro::TokenStream;
use quote::quote;
use syn::{ImplItem, ItemImpl, parse_macro_input};

#[proc_macro_attribute]
pub fn implements_operations(attr: TokenStream, item: TokenStream) -> TokenStream {
    if !attr.is_empty() {
        return syn::Error::new(proc_macro2::Span::call_site(), "takes no arguments")
            .to_compile_error()
            .into();
    }
    let block = parse_macro_input!(item as ItemImpl);
    let methods: Vec<String> = block
        .items
        .iter()
        .filter_map(|item| match item {
            ImplItem::Fn(f) => Some(f.sig.ident.to_string().trim_start_matches("r#").to_owned()),
            _ => None,
        })
        .collect();
    let (impl_generics, _, where_clause) = block.generics.split_for_impl();
    let self_ty = &block.self_ty;
    quote! {
        #block
        impl #impl_generics ::roto_core::ImplementsOperations for #self_ty #where_clause {
            fn implemented_methods() -> &'static [&'static str] {
                &[#(#methods),*]
            }
        }
    }
    .into()
}
