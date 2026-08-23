import { callGenericPopup, POPUP_RESULT, POPUP_TYPE } from '../../../popup.js';
import { translate } from '../../../i18n.js';
import { isAndroidRuntime } from '../../../util/mobile-runtime.js';
import {
    discardCodexAuthImport,
    importCodexAuth,
    openDialog,
    prepareCodexAuthImport,
} from '../../../../tauri-bridge.js';

const ANDROID_CODEX_AUTH_BRIDGE_NAME = 'TauriTavernAndroidImportArchiveBridge';
const ANDROID_CODEX_AUTH_PICKER_RECEIVER = '__TAURITAVERN_CODEX_AUTH_PICKER__';
let androidCodexAuthPickerPending = null;

function createPopupColumn() {
    const root = document.createElement('div');
    root.className = 'flex-container flexFlowColumn';
    root.style.gap = '10px';
    return root;
}

function getAndroidCodexAuthBridge() {
    return window[ANDROID_CODEX_AUTH_BRIDGE_NAME] || null;
}

function ensureAndroidCodexAuthPickerReceiver() {
    if (window[ANDROID_CODEX_AUTH_PICKER_RECEIVER]?.onNativeResult) {
        return;
    }

    window[ANDROID_CODEX_AUTH_PICKER_RECEIVER] = {
        onNativeResult(payload) {
            const pending = androidCodexAuthPickerPending;
            androidCodexAuthPickerPending = null;
            if (!pending) {
                return;
            }

            const error = String(payload?.error || '').trim();
            if (error) {
                pending.reject(new Error(error));
                return;
            }

            const contentUri = String(payload?.content_uri || '').trim();
            pending.resolve(contentUri || null);
        },
    };
}

function pickAndroidCodexAuthContentUri() {
    const bridge = getAndroidCodexAuthBridge();
    if (typeof bridge?.requestCodexAuthPicker !== 'function') {
        throw new Error('Android Codex auth picker is unavailable');
    }
    if (androidCodexAuthPickerPending) {
        throw new Error('Android Codex auth picker is already active');
    }
    ensureAndroidCodexAuthPickerReceiver();

    return new Promise((resolve, reject) => {
        androidCodexAuthPickerPending = { resolve, reject };
        try {
            bridge.requestCodexAuthPicker();
        } catch (error) {
            androidCodexAuthPickerPending = null;
            reject(error);
        }
    });
}

async function pickCodexAuthFile() {
    if (!isAndroidRuntime()) {
        const picked = await openDialog({
            title: translate('Select Codex auth.json'),
            multiple: false,
            directory: false,
            filters: [
                {
                    name: translate('Codex Authentication'),
                    extensions: ['json'],
                },
            ],
        });
        const path = Array.isArray(picked) ? picked[0] : picked;
        const filePath = String(path || '').trim();
        return filePath ? { filePath, staged: false } : null;
    }

    const target = await prepareCodexAuthImport();
    const filePath = String(target?.file_path || '').trim();
    if (!filePath) {
        throw new Error('Codex auth import staging path is missing');
    }

    try {
        const contentUri = await pickAndroidCodexAuthContentUri();
        if (!contentUri) {
            await discardCodexAuthImport(filePath);
            return null;
        }

        const bridge = getAndroidCodexAuthBridge();
        if (typeof bridge?.stageContentUriToFile !== 'function') {
            throw new Error('Android Codex auth staging bridge is unavailable');
        }
        const stagedPath = String(bridge.stageContentUriToFile(contentUri, filePath)).trim();
        if (stagedPath !== filePath) {
            throw new Error('Android Codex auth staging returned an unexpected path');
        }
        return { filePath, staged: true };
    } catch (error) {
        await discardCodexAuthImport(filePath);
        throw error;
    }
}

async function confirmCodexAuthReplacement() {
    const content = createPopupColumn();
    const title = document.createElement('b');
    title.textContent = translate('Replace Codex authentication?');
    const explanation = document.createElement('div');
    explanation.textContent = translate(
        'Valid Codex authentication is already imported. Replace it with the selected auth.json?',
    );
    content.append(title, explanation);

    const result = await callGenericPopup(content, POPUP_TYPE.CONFIRM, '', {
        okButton: translate('Replace'),
        cancelButton: translate('Cancel'),
        allowVerticalScrolling: true,
        wide: false,
        large: false,
    });
    return result === POPUP_RESULT.AFFIRMATIVE;
}

async function showCodexAuthImportSuccess(result) {
    let message;
    if (result.status === 'already_current') {
        message = 'The selected Codex authentication is already imported.';
    } else if (result.status === 'imported') {
        message = result.replaced_existing
            ? 'Codex authentication replaced successfully.'
            : 'Codex authentication imported successfully.';
    } else {
        throw new Error('Codex auth import returned an invalid result');
    }

    await callGenericPopup(translate(message), POPUP_TYPE.TEXT, '', {
        okButton: translate('OK'),
        allowVerticalScrolling: true,
        wide: false,
        large: false,
    });
}

export async function addCodexAuth() {
    const picked = await pickCodexAuthFile();
    if (!picked) {
        return;
    }

    try {
        let result = await importCodexAuth(picked.filePath);
        if (result?.status === 'requires_confirmation') {
            const confirmation = String(result.confirmation || '').trim();
            if (!confirmation) {
                throw new Error('Codex auth replacement confirmation is missing');
            }
            if (!await confirmCodexAuthReplacement()) {
                return;
            }
            result = await importCodexAuth(picked.filePath, confirmation);
        }

        await showCodexAuthImportSuccess(result);
    } finally {
        if (picked.staged) {
            await discardCodexAuthImport(picked.filePath);
        }
    }
}
