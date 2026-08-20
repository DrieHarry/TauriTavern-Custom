import { TAURITAVERN_SETTINGS_BUTTON_ID } from './setting-panel/constants.js';
import { installPairingListener } from './setting-panel/pairing-listener.js';
import { installSyncListeners } from './setting-panel/sync-listeners.js';
import { runOrPopup } from './setting-panel/popup-utils.js';

export function installTauriTavernSettingsPanel() {
    installPairingListener();
    installSyncListeners();

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', () => {
            bindSettingsButton();
            bindDevLogsDrawerButtons();
        }, { once: true });
        return;
    }

    bindSettingsButton();
    bindDevLogsDrawerButtons();
}

function bindSettingsButton() {
    const button = document.getElementById(TAURITAVERN_SETTINGS_BUTTON_ID);
    if (!button) {
        return;
    }

    button.addEventListener('click', () => {
        runOrPopup(async () => {
            const { openTauriTavernSettingsPopup } = await import('./setting-panel/settings-popup.js');
            await openTauriTavernSettingsPopup();
        });
    });
}

function bindDevLogsDrawerButtons() {
    const llmBtn = document.getElementById('dev_logs_llm_btn');
    if (llmBtn) {
        llmBtn.addEventListener('click', () => {
            runOrPopup(async () => {
                const { openLlmApiLogsPanel } = await import('./dev-logs.js');
                await openLlmApiLogsPanel();
            });
        });
    }

    const frontendBtn = document.getElementById('dev_logs_frontend_btn');
    if (frontendBtn) {
        frontendBtn.addEventListener('click', () => {
            runOrPopup(async () => {
                const { openFrontendLogsPanel } = await import('./dev-logs.js');
                await openFrontendLogsPanel();
            });
        });
    }

    const backendBtn = document.getElementById('dev_logs_backend_btn');
    if (backendBtn) {
        backendBtn.addEventListener('click', () => {
            runOrPopup(async () => {
                const { openBackendLogsPanel } = await import('./dev-logs.js');
                await openBackendLogsPanel();
            });
        });
    }
}
