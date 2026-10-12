//! Снятие фоновых процессов — отдельным бинарём, потому что оно НЕОБРАТИМО для всего процесса.
//!
//! `kill_all_background_processes` убивает каждого ребёнка из глобального реестра и навсегда
//! взводит флаг `shutting_down`: после него не спавнится никто. Пока тест жил среди юнит-тестов
//! крейта, он убивал чужих детей — тесты боевого пути `nfqws2` (`nfqws2::run`) получали
//! `ExitedImmediately { code: -1 }` от процесса, которого никто из них не трогал. Здесь, в своём
//! процессе, ему убивать некого.

#![cfg(unix)]

use blockcheckw::system::process::{
    background_shutdown_started, kill_all_background_processes, BackgroundProcess,
};

#[tokio::test]
async fn shutdown_kills_registered_children_and_blocks_new_spawns() {
    let mut process = BackgroundProcess::spawn(&["sleep", "30"]).expect("spawn sleep");
    assert!(process.try_wait().await.is_none());
    assert!(!background_shutdown_started(), "до shutdown флаг снят");

    kill_all_background_processes().await;

    // Пайплайн смотрит на этот флаг, чтобы не слать nft-батчи в таблицу,
    // которую cleanup уже снёс (#66).
    assert!(background_shutdown_started(), "после shutdown флаг взведён");

    assert!(process.try_wait().await.is_some());
    let error =
        BackgroundProcess::spawn(&["sleep", "30"]).expect_err("spawn during shutdown must fail");
    assert!(error.to_string().contains("shutdown in progress"));
}
