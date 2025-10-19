---
description: Analizza il codice e suggerisci correzioni passo-passo senza modificare i file.
mode: agent
---

# 📊 Analisi del codice

Analizza il codice nella workspace corrente (o nel file fornito) e identifica potenziali problemi o aree di miglioramento.  
**Non modificare direttamente i file**. Limìtati ad analizzare e proporre azioni passo-passo.

## Requisiti di output

1. **Sommario esecutivo**  
   - Una breve panoramica (1-2 frasi) dei problemi principali.

2. **Findings ordinati per severità**  
   Classifica i problemi come: Blocker / Critico / Alto / Medio / Basso / Info.  
   Per ogni finding includi:  
   - Titolo breve  
   - File e range di righe (o funzione/class)  
   - Descrizione dettagliata del problema  
   - Evidenza (snippet ≤ 25 righe)  
   - Impatto (sicurezza / performance / affidabilità / manutenibilità)  
   - Confidenza (Alta / Media / Bassa)  
   - **Raccomandazione**:  
     - `No change` (spiega perché non serve modificare)  
     - `Investigate` (servono più dati o test)  
     - `Fix proposed` (fornisci patch suggerita)

3. **Patch proposte** (solo se `Fix proposed`)  
   - Diff in formato unified diff o patch minimale  
   - Spiegazione tecnica  
   - Test da eseguire (comandi o checklist)  
   - Implicazioni backward-compatibility e regressioni possibili

4. **Piano passo-passo**  
   - Sequenza di comandi Git (`checkout`, `apply`, `commit`, `push`)  
   - Comandi di test/lint/build da lanciare  
   - Rollback plan

5. **Controlli automatici raccomandati**  
   - Suggerisci linters, static analyzers, security scanners  
   - Fornisci i comandi concreti per eseguirli

## Vincoli
- Non modificare i file della workspace.  
- Se mancano informazioni sufficienti, usa `Investigate` e spiega cosa manca.  
- Usa esempi concreti (diff, comandi, patch) compatibili col linguaggio del progetto.  

## Variabili disponibili
- File corrente: `${file}`  
- Selezione corrente: `${selection}`  
- Workspace: `${workspaceFolder}`  