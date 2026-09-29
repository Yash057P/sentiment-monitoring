import { Sun, Moon, Languages, Check } from 'lucide-react';
import { useT, LANGUAGES } from '../i18n.jsx';
import { useTheme } from '../theme.jsx';
import { getProfile } from '../api.js';
import Drawer from '../components/Drawer.jsx';
import { TopBar } from '../components/ui.jsx';

export default function SettingsPage() {
  const { t, lang, setLang } = useT();
  const { theme, setTheme } = useTheme();
  const profile = getProfile();

  return (
    <div className="about-page">
      <Drawer />
      <TopBar title="BrandScope" subtitle={t('settings.eyebrow')} />

      <main className="container about">
        <div className="about-hero">
          <span className="eyebrow">{t('settings.eyebrow')}</span>
          <h1>{t('settings.title')}</h1>
        </div>

        <div className="grid two-col">
          <div className="card">
            <h3 className="row" style={{ gap: 8 }}>
              <span className="step-icon"><Sun size={18} /></span> {t('settings.appearance')}
            </h3>
            <p className="card-title">{t('settings.theme')}</p>
            <div className="choice-row">
              <button className={`choice ${theme === 'light' ? 'active' : ''}`} onClick={() => setTheme('light')}>
                <Sun size={20} />
                <b>{t('settings.light')}</b>
                {theme === 'light' && <Check size={16} />}
              </button>
              <button className={`choice ${theme === 'dark' ? 'active' : ''}`} onClick={() => setTheme('dark')}>
                <Moon size={20} />
                <b>{t('settings.dark')}</b>
                {theme === 'dark' && <Check size={16} />}
              </button>
            </div>
            <p className="text-muted" style={{ fontSize: 13, marginTop: 12 }}>
              {t('settings.themeHint')}
            </p>
          </div>

          <div className="card">
            <h3 className="row" style={{ gap: 8 }}>
              <span className="step-icon"><Languages size={18} /></span> {t('settings.language')}
            </h3>
            <p className="card-title">{t('settings.choose')}</p>
            <div className="choice-row col">
              {LANGUAGES.map((l) => (
                <button key={l.code} className={`choice wide ${lang === l.code ? 'active' : ''}`} onClick={() => setLang(l.code)}>
                  <span style={{ width: 34 }}>{l.short}</span>
                  <b>{l.label}</b>
                  {lang === l.code && <Check size={16} />}
                </button>
              ))}
            </div>
          </div>
        </div>

        {profile && (
          <div className="card" style={{ textAlign: 'center' }}>
            <p className="text-muted">
              {t('settings.acct')} <b style={{ color: 'var(--ink)' }}>{profile.display || profile.username}</b>
            </p>
          </div>
        )}
      </main>
    </div>
  );
}