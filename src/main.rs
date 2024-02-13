use std::fs;
use std::env;
use std::process::exit;
use prost::Message;
use substrait::proto;
use substrait::proto::expression::field_reference::ReferenceType;
use substrait::proto::expression::reference_segment;
use substrait::proto::expression::Literal;
use substrait::proto::extensions::SimpleExtensionDeclaration;
use substrait::proto::sort_field::SortKind;
use substrait::proto::AggregateFunction;
use substrait::proto::FunctionArgument;
use substrait::proto::SortField;
use substrait::proto::{Expression,Plan};
use substrait::proto::{plan_rel,Rel,rel::RelType};
use substrait::proto::expression::{MaskExpression,RexType};
use substrait::proto::extensions::simple_extension_declaration::MappingType;
use substrait::proto::function_argument::ArgType;
use substrait::proto::r#type::Kind;

fn print_helper(str: &str, indent: usize) {
    println!("{}{}", " ".repeat(indent), str);
}

fn type_to_str(t: &Kind) -> &str {
    match t {
        Kind::Bool(_) => "Bool",
        Kind::I8(_) => "I8",
        Kind::I16(_) => "I16",
        Kind::I32(_) => "I32",
        Kind::I64(_) => "I64",
        Kind::Fp32(_) => "Fp32",
        Kind::Fp64(_) => "Fp64",
        Kind::String(_) => "String",
        Kind::Binary(_) => "Binary",
        Kind::Timestamp(_) => "Timestamp",
        Kind::Date(_) => "Date",
        Kind::Time(_) => "Time",
        Kind::FixedChar(_) => "FixedChar",
        Kind::Varchar(_) => "Varchar",
        Kind::Struct(_) => "Struct",
        _ => "Etc"
    }
}

fn print_named_struct(ns: &proto::NamedStruct, indent: usize, prefix: &str) {
    print!("{}{}(", " ".repeat(indent), prefix);
    let l = ns.names.len();
    let s = ns.r#struct.as_ref().unwrap();
    for i in 0..l {
        let t = s.types[i].kind.as_ref();
        print!("{} {}, ", type_to_str(t.unwrap()), ns.names[i]);
    }
    println!(")");
}

fn print_readtype(rt: &proto::read_rel::ReadType, indent: usize, prefix: &str) {
    print!("{}{}", " ".repeat(indent), prefix);
    match rt {
        proto::read_rel::ReadType::VirtualTable(t) => {
            print!("VirtualTable {:?}", t.values);
        }
        proto::read_rel::ReadType::LocalFiles(t) => {
            print!("LocalFiles {:?}", t.items);
        }
        proto::read_rel::ReadType::NamedTable(t) => {
            print!("NamedTable {:?}", t.names);
        }
        proto::read_rel::ReadType::ExtensionTable(t) => {
            print!("ExtensionTable {:?}", t.detail);
        }
    }
    println!("");
}

fn print_sortfield(sort: &SortField, exts: &ExtensionLookupTable) {
    print!("sort(");
    if sort.expr.is_some() {
        print_expression(sort.expr.as_ref().unwrap(), exts);
        print!("; ");
    }
    else {
        print!("None; ");
    }
    if sort.sort_kind.is_some() {
        match sort.sort_kind.as_ref().unwrap() {
            SortKind::Direction(sk) => {
                // https://github.com/substrait-io/substrait/blob/main/proto/substrait/algebra.proto:1397
                match *sk {
                    1 => {print!("ASC_NULL_FIRST")}
                    2 => {print!("ASC_NULL_LAST")}
                    3 => {print!("DESC_NULL_FIRST")}
                    4 => {print!("DESC_NULL_LAST")}
                    5 => {print!("CLUSTERED")}
                    _ => {print!("UNSPECIFIED")}
                }
            }
            SortKind::ComparisonFunctionReference(sk) => {
                print!("{}", exts.functions[*sk as usize]);
            }
        }
        print!(")");
    }
    else {
        print!("None)");
    }
}

fn print_funcarg(arg: &FunctionArgument, exts: &ExtensionLookupTable) {
    let arg_type = arg.arg_type.as_ref().unwrap();
    match arg_type {
        ArgType::Enum(a) => {
            print!("{}", a);
        }
        ArgType::Type(a) => {
            print!("{}", type_to_str(a.kind.as_ref().unwrap()));
        }
        ArgType::Value(a) => {
            print_expression(a, exts);
        }
    }
}

fn print_aggregatefunc(aggfunc: &AggregateFunction, exts: &ExtensionLookupTable) {
    let func_ref = aggfunc.function_reference;
    print!("aggr {}(", exts.functions[func_ref as usize]);
    for arg in &aggfunc.arguments {
        print_funcarg(arg, exts);
        print!(", ");
    }
    print!("; ");
    for sort in &aggfunc.sorts {
        print_sortfield(sort, exts);
        print!(", ");
    }
    print!(")");
}

fn print_maskexpression(maskexpr: &MaskExpression) {
    print!("mask[");
    let ss = maskexpr.select.as_ref().unwrap();
    let items: &Vec<proto::expression::mask_expression::StructItem> = ss.struct_items.as_ref();
    for item in items {
        print!("{}, ", item.field);
    }
    print!("]");
}

fn print_expression_literal(literal: &Literal) {
    print!("({:?})", literal.literal_type.as_ref().unwrap());
}

fn print_expression(expr: &Expression, exts: &ExtensionLookupTable) {
    match expr.rex_type.as_ref().unwrap() {
        RexType::Literal(r) => {
            print_expression_literal(r);
        }
        RexType::Selection(r) => {
            print!("(");
            let ref_type = r.reference_type.as_ref().unwrap();
            match ref_type {
                ReferenceType::DirectReference(seg) => {
                    let ref_type2 = seg.reference_type.as_ref().unwrap();
                    match ref_type2 {
                        reference_segment::ReferenceType::MapKey(r) => {
                            print!("mapkey ");
                            print_expression_literal(r.map_key.as_ref().unwrap());
                        }
                        reference_segment::ReferenceType::StructField(r) => {
                            print!("field {}", r.field);
                        }
                        reference_segment::ReferenceType::ListElement(r) => {
                            print!("offset {}", r.offset);
                        }
                    }
                }
                ReferenceType::MaskedReference(masked) => {
                    print_maskexpression(masked);
                }
            }
            print!(")");
        }
        RexType::ScalarFunction(r) => {
            let func_ref = r.function_reference;
            print!("{}(", exts.functions[func_ref as usize]);
            let arguments = &r.arguments;
            for arg in arguments {
                print_funcarg(arg, exts);
                print!(", ");
            }
            print!(")");
        }
        RexType::WindowFunction(r) => {
            print!("WindowFunction {:?}", r);
        }
        RexType::IfThen(r) => {
            print!("IfThen {:?}", r);
        }
        RexType::SwitchExpression(r) => {
            print!("SwitchExpression {:?}", r);
        }
        RexType::SingularOrList(r) => {
            print!("SingularOrList {:?}", r);
        }
        RexType::MultiOrList(r) => {
            print!("MultiOrList {:?}", r);
        }
        RexType::Cast(r) => {
            print!("Cast {:?}", r);
        }
        RexType::Subquery(r) => {
            print!("Subquery {:?}", r);
        }
        RexType::Nested(r) => {
            print!("Nested {:?}", r);
        }
        RexType::Enum(r) => {
            print!("Enum {:?}", r);
        }
    }
}

fn print_rel(rel: &Rel, exts: &ExtensionLookupTable, indent: usize) {
    if rel.rel_type.is_some() {
        let rel_type = rel.rel_type.as_ref().unwrap();
        match rel_type {
            // Zero-input
            RelType::Read(r) => {
                print_helper("Read", indent);
                
                // Table
                print_readtype(r.read_type.as_ref().unwrap(), indent + 1, "table: ");
                
                // Schema
                print_named_struct(r.base_schema.as_ref().unwrap(), indent + 1, "schema: ");
                
                // Filter
                print!("{}filter: ", " ".repeat(indent + 1));
                if r.filter.as_deref().is_some() {
                    print_expression(r.filter.as_deref().unwrap(), exts);
                }
                else {
                    print!("None");
                }
                println!("");
                
                // Projection
                print!("{}projection: ", " ".repeat(indent + 1));
                if r.projection.as_ref().is_some() {
                    print_maskexpression(r.projection.as_ref().unwrap());
                }
                else {
                    print!("None")
                }
                println!("");
            }
            RelType::ExtensionLeaf(r) => {
                print_helper("ExtensionLeaf", indent);
            }
            RelType::Ddl(r) => {
                print_helper("Ddl", indent);
            }
            // One-input
            RelType::Filter(r) => {
                print_helper("Filter", indent);
                if r.condition.is_some() {
                    print!("{}condition: ", " ".repeat(indent));
                    print_expression(r.condition.as_ref().unwrap(), exts);
                    println!("");
                }
                print_rel(&*r.input.as_ref().unwrap(), exts, indent + 1);
            }
            RelType::Fetch(r) => {
                print_helper("Fetch", indent);
                print_rel(&*r.input.as_ref().unwrap(), exts, indent + 1);
            }
            RelType::Aggregate(r) => {
                print_helper("Aggregate", indent);
                print!("{}groups: [", " ".repeat(indent));
                for gr in &r.groupings {
                    print!("[");
                    for expr in &gr.grouping_expressions {
                        print_expression(expr, exts);
                    }
                    print!("]");
                }
                println!("]");
                print!("{}measures: [", " ".repeat(indent));
                for measure in &r.measures {
                    print!("(");
                    print_aggregatefunc(measure.measure.as_ref().unwrap(), exts);
                    if measure.filter.is_some() {
                        print!("; filter ");
                        print_expression(measure.filter.as_ref().unwrap(), exts);
                    }
                    print!(")");
                }
                println!("]");
                print_rel(&*r.input.as_ref().unwrap(), exts, indent + 1);
            },
            RelType::Sort(r) => {
                print_helper("Sort", indent);
                print!("{}sorts: [", " ".repeat(indent));
                for sort in &r.sorts {
                    print_sortfield(sort, exts);
                    print!(", ");
                }
                println!("]");
                print_rel(&*r.input.as_ref().unwrap(), exts, indent + 1);
            }
            RelType::Project(r) => {
                print_helper("Project", indent);
                print!("{}exprs: [", " ".repeat(indent));
                for expr in &r.expressions {
                    print_expression(expr, exts);
                }
                println!("]");
                print_rel(&*r.input.as_ref().unwrap(), exts, indent + 1);
            }
            RelType::ExtensionSingle(r) => {
                print_helper("ExtensionSingle", indent);
                print_rel(&*r.input.as_ref().unwrap(), exts, indent + 1);
            }
            RelType::Write(r) => {
                print_helper("Write", indent);
                print_rel(&*r.input.as_ref().unwrap(), exts, indent + 1);
            }
            RelType::Window(r) => {
                print_helper("Window", indent);
                print_rel(&*r.input.as_ref().unwrap(), exts, indent + 1);
            }
            RelType::Exchange(r) => {
                print_helper("Exchange", indent);
                print_rel(&*r.input.as_ref().unwrap(), exts, indent + 1);
            }
            RelType::Expand(r) => {
                print_helper("Expand", indent);
                print_rel(&*r.input.as_ref().unwrap(), exts, indent + 1);
            }
            // Two-inputs
            RelType::Join(r) => {
                print_helper("Join", indent);
                if r.expression.is_some() {
                    print!("{}expr: ", " ".repeat(indent));
                    print_expression(r.expression.as_ref().unwrap(), exts);
                    println!("");
                }
                if r.post_join_filter.is_some() {
                    print!("{}post-join-filter: ", " ".repeat(indent));
                    print_expression(r.post_join_filter.as_ref().unwrap(), exts);
                    println!("");
                }
                print_rel(&*r.left.as_ref().unwrap(), exts, indent + 1);
                print_helper("with", indent);
                print_rel(&*r.right.as_ref().unwrap(), exts, indent + 1);
            }
            RelType::Cross(r) => {
                print_helper("Cross", indent);
                print_rel(&*r.left.as_ref().unwrap(), exts, indent + 1);
                print_helper("with", indent);
                print_rel(&*r.right.as_ref().unwrap(), exts, indent + 1);
            }
            RelType::HashJoin(r) => {
                print_helper("HashJoin", indent);
                print_rel(&*r.left.as_ref().unwrap(), exts, indent + 1);
                print_helper("with", indent);
                print_rel(&*r.right.as_ref().unwrap(), exts, indent + 1);
            }
            RelType::MergeJoin(r) => {
                print_helper("MergeJoin", indent);
                print_rel(&*r.left.as_ref().unwrap(), exts, indent + 1);
                print_helper("with", indent);
                print_rel(&*r.right.as_ref().unwrap(), exts, indent + 1);
            }
            RelType::NestedLoopJoin(r) => {
                print_helper("NestedLoopJoin", indent);
                print_rel(&*r.left.as_ref().unwrap(), exts, indent + 1);
                print_helper("with", indent);
                print_rel(&*r.right.as_ref().unwrap(), exts, indent + 1);
            }
            // Multiple-inputs
            RelType::Set(r) => {
                print_helper("Set", indent);
                let l = r.inputs.len();
                let mut cnt = 0;
                for i in &r.inputs {
                    print_rel(&i, exts, indent + 1);
                    if cnt < (l - 1) {
                        print_helper(",", indent);
                    }
                    cnt = cnt + 1;
                }
            }
            RelType::ExtensionMulti(r) => {
                print_helper("ExtensionMulti", indent);
                let l = r.inputs.len();
                let mut cnt = 0;
                for i in &r.inputs {
                    print_rel(&i, exts, indent + 1);
                    if cnt < (l - 1) {
                        print_helper(",", indent);
                    }
                    cnt = cnt + 1;
                }
            }
            _ => {
                print_helper("(Not implemented)", indent);
            }
        }
    }
    else {
        print_helper("(NULL)", indent);
    }
}

fn print_everything(plan: &Plan, exts: &ExtensionLookupTable) {
    // relations
    for planrel in &plan.relations {
        match planrel.rel_type.as_ref().unwrap() {
            plan_rel::RelType::Rel(rel) => {
                print_rel(&rel, exts, 0);
            },
            plan_rel::RelType::Root(root) => {
                println!("Root; names({:?})", root.names);
                print_rel(&root.input.as_ref().unwrap(), exts, 1);
            }
        }
    }
}

struct ExtensionLookupTable {
    functions: Vec<String>
}

fn parse_extensions(exts: &Vec<SimpleExtensionDeclaration>) -> ExtensionLookupTable {
    let mut ext_funcs = Vec::new();
    ext_funcs.push(String::from("invalid"));
    for ext in exts {
        match ext.mapping_type.as_ref().unwrap() {
            MappingType::ExtensionType(e) => {

            }
            MappingType::ExtensionTypeVariation(e) => {

            }
            MappingType::ExtensionFunction(e) => {
                let func_id = e.function_anchor;
                assert_eq!(ext_funcs.len(), func_id as usize);
                ext_funcs.push(e.name.clone());
            }
        }
    }
    ExtensionLookupTable{
        functions: ext_funcs
    }
}

fn main() {
    let filename = env::args().nth(1);
    if filename.is_none() {
        println!("Usage: cargo run -- proto_filename");
        exit(1);
    }
    let filename = filename.unwrap();

    let code = fs::read(filename).unwrap();
    let plan = Plan::decode(code.as_slice()).unwrap();
    //println!("{:?}\n", plan);

    let exts = parse_extensions(&plan.extensions);
    print_everything(&plan, &exts);

}
