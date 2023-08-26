use csv::ReaderBuilder;
use std::error::Error;
use std::fs;
use tokio::sync::mpsc;
use tokio::task;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let dir_path = "/home/vinicius/Documents/Pratique/bancos";

    let mut tasks = Vec::new();

    // Canal para coletar resultados das tarefas
    let (tx, mut rx) = mpsc::channel(32); // O número é o tamanho do buffer
    let dir_entries = fs::read_dir(dir_path)?; // Lê todos os arquivos no diretório

    // Cria tarefas assíncronas para cada arquivo
    for entry in dir_entries {
        let entry = entry?;
        let file_path = entry.path();

        // Verifica se o arquivo é um arquivo CSV
        if let Some(extension) = file_path.extension() {
            if extension == "csv" {
                let tx = tx.clone(); // Clone o transmissor para cada tarefa
                let task = task::spawn(async move {
                    let mut ages = Vec::new();
                    if let Err(err) = process_csv_file(file_path, &mut ages) {
                        eprintln!("Error: {:?}", err);
                    }
                    // Envia o resultado da tarefa para a tarefa principal
                    tx.send(ages).await.expect("Failed to send ages");
                });
                tasks.push(task);
            }
        }
    }

    drop(tx);

    // Aguarda a conclusão de todas as tarefas
    for task in tasks {
        task.await?;
    }
    // Coleta e processa os resultados das tarefas
    let mut all_ages = Vec::new();
    while let Some(ages) = rx.recv().await {
        all_ages.extend(ages);
    }
    // Calcula a média das idades
    let average_age: f64 = all_ages.iter().sum::<f64>() / all_ages.len() as f64;
    println!("Average Age of Police Officers: {:.2}", average_age);

    Ok(())
}

fn process_csv_file(
    file_path: std::path::PathBuf,
    ages: &mut Vec<f64>,
) -> Result<(), Box<dyn Error>> {
    let file = fs::File::open(&file_path)?;
    let mut rdr = ReaderBuilder::new().has_headers(true).from_reader(file);

    // Obter índices das colunas
    let profession_index = rdr
        .headers()?
        .iter()
        .position(|h| h == "profession")
        .ok_or("Profession column not found")?;
    let age_index = rdr
        .headers()?
        .iter()
        .position(|h| h == "age")
        .ok_or("Age column not found")?;

    // Loop sobre as linhas do
    for result in rdr.records() {
        let record = result?;
        // Verifica se a linha contém "profession" como "police officer"
        if record.get(profession_index) == Some("police officer") {
            if let Some(age_str) = record.get(age_index) {
                if let Ok(age) = age_str.parse::<f64>() {
                    ages.push(age);
                }
            }
        }
    }

    Ok(())
}
