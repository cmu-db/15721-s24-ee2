use std::fs;
use std::env;
use std::process::exit;
use prost::Message;
use substrait::proto;
use substrait::proto::Plan;
use substrait::proto::{plan_rel,Rel,rel::RelType};
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

fn print_maskexpression(maskexpr: Option<&proto::expression::MaskExpression>, indent: usize, prefix: &str) {
    print!("{}{}", " ".repeat(indent), prefix);
    if maskexpr.is_some() {
        let me = maskexpr.unwrap();
        let ss = me.select.as_ref().unwrap();
        print!("[");
        let items: &Vec<proto::expression::mask_expression::StructItem> = ss.struct_items.as_ref();
        for item in items {
            print!("{}, ", item.field);
        }
        println!("]")
    }
    else {
        println!("None");
    }
}

fn print_expression(expr: Option<&proto::Expression>, indent: usize, prefix: &str) {
    print!("{}{}", " ".repeat(indent), prefix);
    if expr.is_some() {
        let e = expr.unwrap();
        println!("TODO {:?}", e);
    }
    else {
        println!("None");
    }
}

fn print_rel(rel: Rel, indent: usize) {
    if rel.rel_type.is_some() {
        let rel_type = rel.rel_type.unwrap();
        match rel_type {
            // Zero-input
            #[allow(unused)]
            RelType::Read(r) => {
                print_helper("Read", indent);
                // Table
                print_readtype(r.read_type.as_ref().unwrap(), indent + 1, "table: ");
                // Schema
                print_named_struct(r.base_schema.as_ref().unwrap(), indent + 1, "schema: ");
                // Filter
                print_expression(r.filter.as_deref(), indent + 1, "filter: ");
                // Projection
                print_maskexpression(r.projection.as_ref(), indent + 1, "projection: ");
            }
            #[allow(unused)]
            RelType::ExtensionLeaf(r) => {
                print_helper("ExtensionLeaf", indent);
            }
            #[allow(unused)]
            RelType::Ddl(r) => {
                print_helper("Ddl", indent);
            }
            // One-input
            RelType::Filter(r) => {
                print_helper("Filter", indent);
                print_rel(*r.input.unwrap(), indent + 1);
            }
            RelType::Fetch(r) => {
                print_helper("Fetch", indent);
                print_rel(*r.input.unwrap(), indent + 1);
            }
            RelType::Aggregate(r) => {
                print_helper("Aggregate", indent);
                print_rel(*r.input.unwrap(), indent + 1);
            },
            RelType::Sort(r) => {
                print_helper("Sort", indent);
                print_rel(*r.input.unwrap(), indent + 1);
            }
            RelType::Project(r) => {
                print_helper("Project", indent);
                print_rel(*r.input.unwrap(), indent + 1);
            }
            RelType::ExtensionSingle(r) => {
                print_helper("ExtensionSingle", indent);
                print_rel(*r.input.unwrap(), indent + 1);
            }
            RelType::Write(r) => {
                print_helper("Write", indent);
                print_rel(*r.input.unwrap(), indent + 1);
            }
            RelType::Window(r) => {
                print_helper("Window", indent);
                print_rel(*r.input.unwrap(), indent + 1);
            }
            RelType::Exchange(r) => {
                print_helper("Exchange", indent);
                print_rel(*r.input.unwrap(), indent + 1);
            }
            RelType::Expand(r) => {
                print_helper("Expand", indent);
                print_rel(*r.input.unwrap(), indent + 1);
            }
            // Two-inputs
            RelType::Join(r) => {
                print_helper("Join", indent);
                print_rel(*r.left.unwrap(), indent + 1);
                print_helper("- with", indent);
                print_rel(*r.right.unwrap(), indent + 1);
            }
            RelType::Cross(r) => {
                print_helper("Cross", indent);
                print_rel(*r.left.unwrap(), indent + 1);
                print_helper("- with", indent);
                print_rel(*r.right.unwrap(), indent + 1);
            }
            RelType::HashJoin(r) => {
                print_helper("HashJoin", indent);
                print_rel(*r.left.unwrap(), indent + 1);
                print_helper("- with", indent);
                print_rel(*r.right.unwrap(), indent + 1);
            }
            RelType::MergeJoin(r) => {
                print_helper("MergeJoin", indent);
                print_rel(*r.left.unwrap(), indent + 1);
                print_helper("- with", indent);
                print_rel(*r.right.unwrap(), indent + 1);
            }
            RelType::NestedLoopJoin(r) => {
                print_helper("NestedLoopJoin", indent);
                print_rel(*r.left.unwrap(), indent + 1);
                print_helper("- with", indent);
                print_rel(*r.right.unwrap(), indent + 1);
            }
            // Multiple-inputs
            RelType::Set(r) => {
                print_helper("Set", indent);
                let l = r.inputs.len();
                let mut cnt = 0;
                for i in r.inputs {
                    print_rel(i, indent + 1);
                    if cnt < (l - 1) {
                        print_helper("- and", indent);
                    }
                    cnt = cnt + 1;
                }
            }
            RelType::ExtensionMulti(r) => {
                print_helper("ExtensionMulti", indent);
                let l = r.inputs.len();
                let mut cnt = 0;
                for i in r.inputs {
                    print_rel(i, indent + 1);
                    if cnt < (l - 1) {
                        print_helper("- and", indent);
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

fn print_everything(plan: Plan) {
    // relations
    for planrel in plan.relations {
        match planrel.rel_type.unwrap() {
            plan_rel::RelType::Rel(rel) => {
                print_rel(rel, 0);
            },
            plan_rel::RelType::Root(root) => {
                println!("Root; names({:?})", root.names);
                if root.input.is_some() {
                    print_rel(root.input.unwrap(), 1);
                }
            }
        }
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
    println!("{:?}\n", plan);

    print_everything(plan)

}
