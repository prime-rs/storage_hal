use proc_macro::TokenStream;
use quote::quote;

#[proc_macro_derive(StorageData)]
pub fn storage_data_macro_derive(input: TokenStream) -> TokenStream {
    let ast = syn::parse(input).unwrap();
    impl_storage_data_macro(&ast)
}

fn impl_storage_data_macro(ast: &syn::DeriveInput) -> TokenStream {
    let name = &ast.ident;
    let gen = quote! {
        impl StorageData for #name {
            fn name() -> String {
                stringify!(#name).to_string()
            }
        }
    };
    gen.into()
}
