import { useState } from 'react';
import { ArrowRight, User, Eye, EyeOff, Radio, Tag, TrendingUp, ShieldCheck } from 'lucide-react';
import { login, saveSession } from '../api.js';
import { useT } from '../i18n.jsx';
import { companies } from '../components/companies.js';
import Drawer from '../components/Drawer.jsx';
import { NavLinks, ThemeToggle } from '../components/ui.jsx';

const FEATURES = [
  { icon: Radio, key: 'f1' },
  { icon: Tag, key: 'f2' },
  { icon: TrendingUp, key: 'f3' },
];

export default function LoginPage({ onLogin }) {
  const { t } = useT();
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [showPw, setShowPw] = useState(false);
  const [error, setError] = useState('');
  const [busy, setBusy] = useState(false);

  async function submit() {
    if (busy) return;
    if (!username.trim() || !password) {
      setError(t('login.enter'));
      return;
    }
    setBusy(true);
    setError('');
    try {
      const data = await login(username.trim(), password);
      saveSession(data);
      onLogin(data);
    } catch {
      setError(t('login.wrong'));
    } finally {
      setBusy(false);
    }
  }

  function pick(handle) {
    setUsername(handle);
    setPassword(`${handle}123`);
    setError('');
  }

  return (
    <div className="login-page">
      <div className="login-topbar">
        <Drawer />
        <span className="brand">
          <span className="logo-badge">📊</span> BrandScope
        </span>
        <div className="spacer" />
        <NavLinks />
        <div className="spacer" />
        <ThemeToggle />
      </div>

      <div className="login-body">
      <section className="hero-panel">
        <div className="hero-orb orb1" />
        <div className="hero-orb orb2" />
        <div className="hero-orb orb3" />

        <div className="hero-brand">
          <span className="logo-badge lg">📊</span>
          <span className="hero-name">BrandScope</span>
        </div>

        <div className="hero-copy">
          <span className="eyebrow light">{t('login.eyebrow')}</span>
          <h1>{t('login.title')}</h1>
          <p>{t('login.lede')}</p>
        </div>

        <div className="hero-features">
          {FEATURES.map((f) => {
            const Icon = f.icon;
            return (
              <div className="hf" key={f.key}>
                <span className="hf-icon"><Icon size={17} /></span>
                <span>
                  <b>{t(`login.${f.key}t`)}</b>
                  <small>{t(`login.${f.key}d`)}</small>
                </span>
              </div>
            );
          })}
        </div>

        <div className="hero-marquee">
          <small className="text-muted" style={{ color: 'rgba(255,255,255,0.75)', display: 'block', marginBottom: 10 }}>
            {t('login.powered')}
          </small>
          <div className="marquee">
            {[...companies, ...companies].map((c, i) => (
              <span key={`${c.handle}-${i}`} className="m-chip" style={{ color: c.color }}>
                {c.logo} {c.name}
              </span>
            ))}
          </div>
        </div>
      </section>

      <section className="form-panel">
        <div className="login-card">
          <span className="eyebrow" style={{ color: 'var(--brand)' }}>{t('login.eyebrow')}</span>
          <h1 style={{ fontSize: '1.5rem', margin: '6px 0 4px' }}>BrandScope</h1>
          <p className="text-muted" style={{ margin: '0 0 20px' }}>
            {t('nav.dashboard')} → {t('overview.outlook')}
          </p>

          {error && <div className="form-error">{error}</div>}

          <div className="field">
            <label>{t('login.username')}</label>
            <div className="input-wrap">
              <User size={17} className="inp-ic" />
              <input
                autoFocus
                value={username}
                onChange={(e) => setUsername(e.target.value)}
                onKeyDown={(e) => e.key === 'Enter' && submit()}
                placeholder={t('login.usernamePh')}
              />
            </div>
          </div>

          <div className="field">
            <label>{t('login.password')}</label>
            <div className="input-wrap">
              <ShieldCheck size={17} className="inp-ic" />
              <input
                type={showPw ? 'text' : 'password'}
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                onKeyDown={(e) => e.key === 'Enter' && submit()}
                placeholder={t('login.passwordPh')}
              />
              <button className="eye-btn" onClick={() => setShowPw((s) => !s)} aria-label="Toggle password">
                {showPw ? <EyeOff size={17} /> : <Eye size={17} />}
              </button>
            </div>
          </div>

          <button className="btn block pulse-btn" disabled={busy} onClick={submit}>
            {busy ? t('login.signing') : t('login.signin')}
            {!busy && <ArrowRight size={16} />}
          </button>

          <div className="or-divider"><span>{t('login.quick')}</span></div>

          <div className="chip-grid">
            {companies.map((c) => (
              <button key={c.handle} className="company-chip" style={{ '--chip-color': c.color }} onClick={() => pick(c.handle)}>
                <span>{c.logo}</span>
                <span className="cc-name">{c.name}</span>
                <span className="cc-creds">{c.handle} / {c.handle}123</span>
              </button>
            ))}
          </div>

          <button className="admin-chip" onClick={() => pick('admin')}>
            <ShieldCheck size={15} /> admin / admin · {t('login.adminHint')}
          </button>
        </div>
      </section>
      </div>
    </div>
  );
}