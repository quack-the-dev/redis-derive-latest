use crate::util::{self, ParsedAttributeMap};
use quote::quote;
use syn::{DataStruct, Fields, Ident};

pub fn derive_to_redis_struct(
    data_struct: DataStruct,
    type_ident: Ident,
    attrs: ParsedAttributeMap,
) -> proc_macro::TokenStream {
    match &data_struct.fields {
        Fields::Named(fields_named) => {
            let mut regular_fields: Vec<(&syn::Ident, String, bool)> = Vec::new();

            for field in &fields_named.named {
                let field_ident = field.ident.as_ref().expect("Named field should have ident");
                let field_attrs = util::parse_field_attributes(&field.attrs);

                if field_attrs.skip {
                    continue;
                }

                let field_name = util::transform_field_name(
                    &field_ident.to_string(),
                    attrs.rename_all.as_ref(),
                    field_attrs.rename.as_ref(),
                );

                let is_optional = util::is_optional(&field.ty);
                regular_fields.push((field_ident, field_name.clone(), is_optional));
            }

            let (field_idents, field_names, field_is_optionals): (Vec<_>, Vec<_>, Vec<_>) = {
                let mut __ids = Vec::new();
                let mut __names = Vec::new();
                let mut __opts = Vec::new();
                for (i, n, o) in regular_fields {
                    __ids.push(i);
                    __names.push(n);
                    __opts.push(o);
                }
                (__ids, __names, __opts)
            };

            // Build per-field tokens to avoid type-mismatch in branches
            let write_kvs: Vec<proc_macro2::TokenStream> = field_idents
                .iter()
                .zip(field_names.iter())
                .zip(field_is_optionals.iter())
                .map(|((ident, name), is_opt)| {
                    if *is_opt {
                        quote! {
                            out.write_arg(#name.as_bytes());
                            match &self.#ident {
                                Some(__value) => { __value.write_redis_args(out); }
                                None => { out.write_arg(b"null"); }
                            }
                        }
                    } else {
                        quote! {
                            out.write_arg(#name.as_bytes());
                            (&self.#ident).write_redis_args(out);
                        }
                    }
                })
                .collect();

            let num_args_tokens: Vec<proc_macro2::TokenStream> = field_idents
                .iter()
                .zip(field_names.iter())
                .zip(field_is_optionals.iter())
                .map(|((ident, _name), is_opt)| {
                    if *is_opt {
                        quote! {
                            count += 1; // field name
                            match &self.#ident {
                                Some(__value) => { count += __value.num_of_args(); }
                                None => { count += 1; } // the literal "null"
                            }
                        }
                    } else {
                        quote! {
                            count += 1; // field name
                            count += (&self.#ident).num_of_args(); // field value args
                        }
                    }
                })
                .collect();

            // Generate the basic ToRedisArgs implementation
            let to_redis_impl = quote! {
                impl redis::ToRedisArgs for #type_ident {
                    fn write_redis_args<W: ?Sized + redis::RedisWrite>(&self, out: &mut W) {
                        // Write each field as key-value pairs for hash storage
                        #(#write_kvs)*
                    }

                    fn num_of_args(&self) -> usize {
                        let mut count = 0;
                        #(#num_args_tokens)*
                        count
                    }
                }
            };

            // Build per-field body for to_hset_pairs to avoid type mismatches
            let to_hset_pairs_body: Vec<proc_macro2::TokenStream> = field_idents
                .iter()
                .zip(field_names.iter())
                .zip(field_is_optionals.iter())
                .map(|((ident, name), is_opt)| {
                    if *is_opt {
                        quote! {
                            match &self.#ident {
                                Some(__value) => {
                                    let __args = redis::ToRedisArgs::to_redis_args(__value);
                                    if __args.len() == 1 {
                                        __pairs.push((#name.to_string(), __args.into_iter().next().unwrap()));
                                    } else {
                                        let mut __combined: Vec<u8> = Vec::new();
                                        for __a in __args {
                                            if !__combined.is_empty() { __combined.push(b' '); }
                                            __combined.extend_from_slice(&__a);
                                        }
                                        __pairs.push((#name.to_string(), __combined));
                                    }
                                }
                                None => {
                                    __pairs.push((#name.to_string(), b"null".to_vec()));
                                }
                            }
                        }
                    } else {
                        quote! {
                            let __args = redis::ToRedisArgs::to_redis_args(&self.#ident);
                            if __args.len() == 1 {
                                __pairs.push((#name.to_string(), __args.into_iter().next().unwrap()));
                            } else {
                                let mut __combined: Vec<u8> = Vec::new();
                                for __a in __args {
                                    if !__combined.is_empty() { __combined.push(b' '); }
                                    __combined.extend_from_slice(&__a);
                                }
                                __pairs.push((#name.to_string(), __combined));
                            }
                        }
                    }
                })
                .collect();

            // Generate inherent method to_hset_pairs for convenient hset_multiple usage
            let to_hset_pairs_impl = quote! {
                impl #type_ident {
                    pub fn to_hset_pairs(&self) -> Vec<(String, Vec<u8>)> {
                        let mut __pairs: Vec<(String, Vec<u8>)> = Vec::new();
                        #(#to_hset_pairs_body)*
                        __pairs
                    }
                }
            };

            let expanded = quote! {
                #to_redis_impl
                #to_hset_pairs_impl
            };

            expanded.into()
        }
        Fields::Unnamed(fields_unnamed) => {
            let field_count = fields_unnamed.unnamed.len();
            let indices: Vec<usize> = (0..field_count).collect();

            let to_redis_impl = quote! {
                impl redis::ToRedisArgs for #type_ident {
                    fn write_redis_args<W: ?Sized + redis::RedisWrite>(&self, out: &mut W) {
                        // Write tuple struct fields as an array
                        #(
                            (&self.#indices).write_redis_args(out);
                        )*
                    }

                    fn num_of_args(&self) -> usize {
                        let mut count = 0;
                        #(
                            count += (&self.#indices).num_of_args();
                        )*
                        count
                    }
                }
            };

            to_redis_impl.into()
        }
        Fields::Unit => {
            let to_redis_impl = quote! {
                impl redis::ToRedisArgs for #type_ident {
                    fn write_redis_args<W: ?Sized + redis::RedisWrite>(&self, _out: &mut W) {
                        // Unit structs don't write any args
                    }

                    fn num_of_args(&self) -> usize {
                        0
                    }
                }
            };

            to_redis_impl.into()
        }
    }
}

pub fn derive_from_redis_struct(
    data_struct: DataStruct,
    type_ident: Ident,
    attrs: ParsedAttributeMap,
) -> proc_macro::TokenStream {
    match &data_struct.fields {
        Fields::Named(fields_named) => {
            let mut regular_fields: Vec<(&syn::Ident, String, bool)> = Vec::new();

            for field in &fields_named.named {
                let field_ident = field.ident.as_ref().expect("Named field should have ident");
                let field_attrs = util::parse_field_attributes(&field.attrs);

                if field_attrs.skip {
                    continue;
                }

                let field_name = util::transform_field_name(
                    &field_ident.to_string(),
                    attrs.rename_all.as_ref(),
                    field_attrs.rename.as_ref(),
                );

                let is_optional = util::is_optional(&field.ty);
                regular_fields.push((field_ident, field_name, is_optional));
            }

            let (field_idents, field_names, field_is_optionals): (Vec<_>, Vec<_>, Vec<_>) = {
                let mut __ids = Vec::new();
                let mut __names = Vec::new();
                let mut __opts = Vec::new();
                for (i, n, o) in regular_fields {
                    __ids.push(i);
                    __names.push(n);
                    __opts.push(o);
                }
                (__ids, __names, __opts)
            };

            // Build per-field assignment tokens (avoid mixing Option and non-Option types)
            let assign_tokens: Vec<proc_macro2::TokenStream> = field_idents
                .iter()
                .zip(field_names.iter())
                .zip(field_is_optionals.iter())
                .map(|((ident, name), is_opt)| {
                    if *is_opt {
                        quote! {
                            #ident: {
                                match fields_map.get(#name) {
                                    Some(value) => {
                                        let __is_null = match value {
                                            redis::Value::Nil => true,
                                            redis::Value::BulkString(data) => data.as_slice() == b"null",
                                            redis::Value::SimpleString(s) => s == "null",
                                            redis::Value::VerbatimString { text, .. } => text == "null",
                                            _ => false,
                                        };
                                        if __is_null {
                                            None
                                        } else {
                                            Some(redis::FromRedisValue::from_redis_value(value)
                                                .map_err(|e| redis::RedisError::from((
                                                    redis::ErrorKind::TypeError,
                                                    "Failed to parse field",
                                                    format!("Field '{}': {}", #name, e),
                                                )))?)
                                        }
                                    }
                                    None => None,
                                }
                            }
                        }
                    } else {
                        quote! {
                            #ident: {
                                match fields_map.get(#name) {
                                    Some(value) => redis::FromRedisValue::from_redis_value(value)
                                        .map_err(|e| redis::RedisError::from((
                                            redis::ErrorKind::TypeError,
                                            "Failed to parse field",
                                            format!("Field '{}': {}", #name, e),
                                        )))?,
                                    None => return Err(redis::RedisError::from((
                                        redis::ErrorKind::TypeError,
                                        "Missing required field",
                                        #name.to_string(),
                                    ))),
                                }
                            }
                        }
                    }
                })
                .collect();

            let from_redis_impl = quote! {
                impl redis::FromRedisValue for #type_ident {
                    fn from_redis_value(v: &redis::Value) -> redis::RedisResult<Self> {
                        match v {
                            redis::Value::Array(items) if items.len() % 2 == 0 => {
                                let mut fields_map = std::collections::HashMap::new();
                                
                                // Parse key-value pairs from array
                                for chunk in items.chunks(2) {
                                    let key: String = redis::FromRedisValue::from_redis_value(&chunk[0])?;
                                    fields_map.insert(key, &chunk[1]);
                                }

                                Ok(Self { #(#assign_tokens),* })
                            }
                            redis::Value::Map(map) => {
                                // Handle Redis hash/map type (RESP3)
                                let mut fields_map = std::collections::HashMap::new();
                                
                                for (key, value) in map {
                                    let key_str: String = redis::FromRedisValue::from_redis_value(key)?;
                                    fields_map.insert(key_str, value);
                                }

                                Ok(Self { #(#assign_tokens),* })
                            }
                            redis::Value::Nil => {
                                Err(redis::RedisError::from((
                                    redis::ErrorKind::TypeError,
                                    "Cannot deserialize struct from nil value",
                                )))
                            }
                            _ => {
                                Err(redis::RedisError::from((
                                    redis::ErrorKind::TypeError,
                                    "Expected Array or Map for struct",
                                )))
                            }
                        }
                    }
                }
            };

            from_redis_impl.into()
        }
        Fields::Unnamed(fields_unnamed) => {
            let field_count = fields_unnamed.unnamed.len();
            let indices: Vec<syn::Index> = (0..field_count)
                .map(syn::Index::from)
                .collect();

            let from_redis_impl = quote! {
                impl redis::FromRedisValue for #type_ident {
                    fn from_redis_value(v: &redis::Value) -> redis::RedisResult<Self> {
                        match v {
                            redis::Value::Array(items) => {
                                if items.len() != #field_count {
                                    return Err(redis::RedisError::from((
                                        redis::ErrorKind::TypeError,
                                        "Array length mismatch",
                                        format!("Expected {} elements, got {}", #field_count, items.len()),
                                    )));
                                }

                                Ok(Self(
                                    #(
                                        redis::FromRedisValue::from_redis_value(&items[#indices])
                                            .map_err(|e| redis::RedisError::from((
                                                redis::ErrorKind::TypeError,
                                                "Failed to parse tuple element",
                                                format!("At index {}: {}", #indices, e),
                                            )))?,
                                    )*
                                ))
                            }
                            redis::Value::Nil => {
                                Err(redis::RedisError::from((
                                    redis::ErrorKind::TypeError,
                                    "Cannot deserialize tuple struct from nil",
                                )))
                            }
                            _ => {
                                Err(redis::RedisError::from((
                                    redis::ErrorKind::TypeError,
                                    "Expected Array for tuple struct",
                                )))
                            }
                        }
                    }
                }
            };

            from_redis_impl.into()
        }
        Fields::Unit => {
            let from_redis_impl = quote! {
                impl redis::FromRedisValue for #type_ident {
                    fn from_redis_value(_v: &redis::Value) -> redis::RedisResult<Self> {
                        Ok(Self)
                    }
                }
            };

            from_redis_impl.into()
        }
    }
}