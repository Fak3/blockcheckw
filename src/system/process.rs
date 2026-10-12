use std::os::unix::process::CommandExt;
use std::sync::{Arc, Mutex as StdMutex, OnceLock, Weak};
use std::time::Duration;
use tokio::process::Command;
use tokio::sync::Mutex;
use tokio::time::timeout;

use crate::error::BlockcheckError;

#[derive(Debug)]
pub struct ProcessResult {
    pub exit_code: i32,
    pub stdout: String,
    pub stderr: String,
}

/// Run a command synchronously (blocking), capturing stdout/stderr.
/// Used for short-lived commands like nft, curl.
pub async fn run_process(args: &[&str], timeout_ms: u64) -> Result<ProcessResult, BlockcheckError> {
    let (program, cmd_args) = args
        .split_first()
        .ok_or_else(|| BlockcheckError::ProcessSpawn {
            reason: "empty command".to_string(),
        })?;

    let fut = Command::new(program)
        .args(cmd_args)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .output();

    let output = timeout(Duration::from_millis(timeout_ms), fut)
        .await
        .map_err(|_| BlockcheckError::ProcessTimeout { timeout_ms })?
        .map_err(|e| BlockcheckError::ProcessSpawn {
            reason: e.to_string(),
        })?;

    Ok(ProcessResult {
        exit_code: output.status.code().unwrap_or(-1),
        stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
    })
}

/// Run a command with stdin input, capturing stdout/stderr.
pub async fn run_process_stdin(
    args: &[&str],
    stdin_data: &str,
    timeout_ms: u64,
) -> Result<ProcessResult, BlockcheckError> {
    use tokio::io::AsyncWriteExt;

    let (program, cmd_args) = args
        .split_first()
        .ok_or_else(|| BlockcheckError::ProcessSpawn {
            reason: "empty command".to_string(),
        })?;

    let mut child = Command::new(program)
        .args(cmd_args)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .map_err(|e| BlockcheckError::ProcessSpawn {
            reason: e.to_string(),
        })?;

    let mut stdin = child.stdin.take().expect("stdin piped");
    let data = stdin_data.to_string();
    tokio::spawn(async move {
        // best-effort — pipe may be broken if process exited early
        let _ = stdin.write_all(data.as_bytes()).await;
        drop(stdin);
    });

    let output = timeout(Duration::from_millis(timeout_ms), child.wait_with_output())
        .await
        .map_err(|_| BlockcheckError::ProcessTimeout { timeout_ms })?
        .map_err(|e| BlockcheckError::ProcessSpawn {
            reason: e.to_string(),
        })?;

    Ok(ProcessResult {
        exit_code: output.status.code().unwrap_or(-1),
        stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
    })
}

#[derive(Default)]
struct BackgroundRegistry {
    shutting_down: bool,
    children: Vec<Weak<Mutex<tokio::process::Child>>>,
}

fn background_registry() -> &'static StdMutex<BackgroundRegistry> {
    static REGISTRY: OnceLock<StdMutex<BackgroundRegistry>> = OnceLock::new();
    REGISTRY.get_or_init(|| StdMutex::new(BackgroundRegistry::default()))
}

/// Началось ли снятие фоновых процессов. Пайплайн спрашивает об этом перед
/// каждым батчем: после Ctrl+C cleanup сносит нашу nft-таблицу раньше, чем
/// пайплайн успевает остановиться (#66).
pub fn background_shutdown_started() -> bool {
    background_registry().lock().unwrap().shutting_down
}

/// Prevent new background children from being spawned during shutdown.
pub fn begin_background_shutdown() {
    background_registry().lock().unwrap().shutting_down = true;
}

fn registered_children() -> Vec<Arc<Mutex<tokio::process::Child>>> {
    let mut registry = background_registry().lock().unwrap();
    registry.shutting_down = true;
    registry.children.retain(|child| child.strong_count() > 0);
    registry.children.iter().filter_map(Weak::upgrade).collect()
}

/// Kill and reap every live child spawned through [`BackgroundProcess`].
pub async fn kill_all_background_processes() {
    for child in registered_children() {
        let _ = child.lock().await.kill().await;
    }
}

/// Best-effort synchronous kill initiation for panic hooks.
pub fn start_kill_all_background_processes() {
    for child in registered_children() {
        if let Ok(mut child) = child.try_lock() {
            let _ = child.start_kill();
        }
    }
}

/// Попросить ядро убить ребёнка, когда умрёт МЫ — включая смерть по `SIGKILL`.
///
/// Ставится на всякий порождаемый нами долгоживущий процесс: `kill_on_drop`, обработчики
/// `SIGINT`/`SIGTERM` и `panic`-хук суть код, исполняемый умирающим процессом, а `SIGKILL`
/// не даёт исполнить ничего. Так уходит продукт третьего невода (он шлёт `SIGKILL`, когда
/// подбор пережил свою надобность), так приходит OOM-killer и `kill -9` от человека — и на
/// очереди остаётся живой `nfqws2`, а в ядре наша таблица.
///
/// `PDEATHSIG` сбрасывается ядром при `exec` только у setuid/setgid-бинарей; движок не такой,
/// и просьба переживает `exec`.
pub fn die_with_parent(command: &mut std::process::Command) {
    let parent_pid = std::process::id() as libc::pid_t;
    // SAFETY: между `fork` и `exec` дозволено звать только async-signal-safe функции.
    // `prctl`, `getppid` и `_exit` — все три из этого списка; аллокаций, локов и обращений
    // к рантайму здесь нет.
    unsafe {
        command.pre_exec(move || {
            if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) != 0 {
                return Err(std::io::Error::last_os_error());
            }
            // Родитель мог умереть в окне между `fork` и `prctl` — тогда сигнал не придёт уже
            // никогда, и ребёнок остался бы сиротой при взведённом `PDEATHSIG`. Гонка
            // закрывается сверкой, а не порядком строк.
            if libc::getppid() != parent_pid {
                libc::_exit(0);
            }
            Ok(())
        });
    }
}

/// Сколько последних байт stderr фонового процесса мы помним. Причина отказа движка — это
/// его последние строки; весь поток держать незачем, а на роутере и не на что.
const STDERR_TAIL_BYTES: usize = 4096;
const STDERR_DRAIN_TIMEOUT_MS: u64 = 200;

/// Хвост stderr: ограниченный буфер, который вычерпывающая задача пополняет до EOF.
#[derive(Debug, Default)]
struct StderrTail(StdMutex<Vec<u8>>);

impl StderrTail {
    fn push(&self, chunk: &[u8]) {
        let mut tail = self.0.lock().unwrap();
        tail.extend_from_slice(chunk);
        let excess = tail.len().saturating_sub(STDERR_TAIL_BYTES);
        tail.drain(..excess);
    }

    fn text(&self) -> String {
        String::from_utf8_lossy(&self.0.lock().unwrap())
            .trim()
            .to_string()
    }
}

/// A registered background process handle. Wraps a tokio process child.
#[derive(Debug)]
pub struct BackgroundProcess {
    child: Arc<Mutex<tokio::process::Child>>,
    stderr: Arc<StderrTail>,
    drain: Option<tokio::task::JoinHandle<()>>,
}

impl BackgroundProcess {
    /// Spawn a background process: stdout → /dev/null, stderr — в ограниченный хвост.
    ///
    /// Пайп stderr вычерпывается задачей всё время жизни процесса. Без неё нельзя: процесс,
    /// написавший больше ёмкости пайпа (64 КиБ), встал бы на записи, а мы бы приняли его за
    /// живой. Прежде ради этого stderr слали в /dev/null — и отказ движка на боевом пути не
    /// видел никто (#95).
    pub fn spawn(args: &[&str]) -> Result<Self, BlockcheckError> {
        let (program, cmd_args) =
            args.split_first()
                .ok_or_else(|| BlockcheckError::ProcessSpawn {
                    reason: "empty command".to_string(),
                })?;

        // Holding the registry lock across spawn makes registration atomic with
        // begin_background_shutdown(): cleanup cannot miss a concurrently
        // created child, and no child can start after shutdown begins.
        let mut registry = background_registry().lock().unwrap();
        if registry.shutting_down {
            return Err(BlockcheckError::ProcessSpawn {
                reason: "shutdown in progress".to_string(),
            });
        }

        // СМЕРТЬ ВМЕСТЕ С РОДИТЕЛЕМ, И ЭТО НЕ ДУБЛИРОВАНИЕ `kill_on_drop`.
        //
        // `kill_on_drop`, обработчики `SIGINT`/`SIGTERM` и `panic`-хук — всё это код, который
        // исполняет УМИРАЮЩИЙ процесс. По `SIGKILL` не исполняется ничего, а именно его шлёт
        // продукт третьего невода, когда подбор пережил свою надобность; так же приходит
        // OOM-killer и `kill -9` от человека. Без `PDEATHSIG` на очереди остаётся живой
        // `nfqws2`, в ядре — наша таблица, продукт слепнет, а трафик человека встаёт целиком.
        //
        let mut std_cmd = std::process::Command::new(program);
        std_cmd
            .args(cmd_args)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::piped());
        die_with_parent(&mut std_cmd);

        let mut child = Command::from(std_cmd)
            .kill_on_drop(true)
            .spawn()
            .map_err(|e| BlockcheckError::ProcessSpawn {
                reason: e.to_string(),
            })?;

        let stderr = Arc::new(StderrTail::default());
        let drain = child.stderr.take().map(|mut pipe| {
            let tail = Arc::clone(&stderr);
            tokio::spawn(async move {
                use tokio::io::AsyncReadExt;
                let mut buf = [0u8; 4096];
                while let Ok(n @ 1..) = pipe.read(&mut buf).await {
                    tail.push(&buf[..n]);
                }
            })
        });

        let child = Arc::new(Mutex::new(child));
        registry.children.retain(|child| child.strong_count() > 0);
        registry.children.push(Arc::downgrade(&child));

        Ok(Self {
            child,
            stderr,
            drain,
        })
    }

    /// Хвост stderr, накопленный к этому моменту. Для живого процесса — снимок.
    pub fn stderr_tail(&self) -> String {
        self.stderr.text()
    }

    /// Хвост stderr вышедшего процесса — дочитанный до EOF.
    ///
    /// После выхода в пайпе ещё могут лежать непрочитанные последние строки, а они-то и
    /// называют причину. Ждём вычерпывающую задачу, но с потолком: пишущий конец мог
    /// унаследовать внук процесса, и тогда EOF не придёт никогда.
    pub async fn stderr_after_exit(&mut self) -> String {
        if let Some(drain) = self.drain.take() {
            let _ = timeout(Duration::from_millis(STDERR_DRAIN_TIMEOUT_MS), drain).await;
        }
        self.stderr.text()
    }

    /// Kill the process (SIGKILL) and wait to reap the zombie.
    pub async fn kill(&mut self) {
        // best-effort — process may have already exited
        let _ = self.child.lock().await.kill().await;
    }

    /// Pid живого ребёнка. `None` — уже реапнут: pid реапнутого процесса называть нельзя,
    /// его успели переиспользовать под чужой процесс, и убивать по нему — убивать чужое.
    pub fn pid(&self) -> Option<u32> {
        self.child.try_lock().ok().and_then(|child| child.id())
    }

    /// Check if the process is still running.
    pub async fn try_wait(&mut self) -> Option<i32> {
        match self.child.lock().await.try_wait() {
            Ok(Some(status)) => Some(status.code().unwrap_or(-1)),
            _ => None,
        }
    }
}
