import { useEffect, useState } from 'react';
import { useLocation, useNavigate } from 'react-router-dom';
import { LayoutDashboard, ShieldCheck, Info, Settings as SettingsIcon, LogIn, LogOut, X, Sun, Moon } from 'lucide-react';
import { useT } from '../i18n.jsx';
import { useTheme } from '../theme.jsx';
import { getProfile, clearSession } from '../api.js';

export default function Drawer() {
  const { t, lang, setLang } = useT();
  const { theme, setTheme } = useTheme();
  const navigate = useNavigate();
  const location = useLocation();
  const [open, setOpen] = useState(false);
  const profile = getProfile();

  useEffect(() => {
    document.body.style.overflow = open ? 'hidden' : '';
    return () => {
      document.body.style.overflow = '';
    };
  }, [open]);

  const items = !profile
    ? [
        { to: '/login', icon: LogIn, label: t('nav.login') },
        { to: '/about', icon: Info, label: t('nav.about') },
        { to: '/settings', icon: SettingsIcon, label: t('nav.settings') },
      ]
    : [
        { to: profile.role === 'admin' ? '/admin' : '/company', icon: profile.role === 'admin' ? ShieldCheck : LayoutDashboard, label: profile.role === 'admin' ? t('nav.admin') : t('nav.dashboard') },
        { to: '/about', icon: Info, label: t('nav.about') },
        { to: '/settings', icon: SettingsIcon, label: t('nav.settings') },
      ];

  function go(to) {
    navigate(to);
    setOpen(false);
  }

  function logout() {
    clearSession();
    setOpen(false);
    navigate('/login', { replace: true });
  }

  const M = ['en', 'hi', 'mr'];

  return (
    <>
      <button className="hamburger" aria-label="Menu" onClick={() => setOpen(true)}>
        <span />
        <span />
        <span />
      </button>

      {open && <div className="drawer-backdrop" onClick={() => setOpen(false)} />}

      <aside className={`drawer ${open ? 'open' : ''}`} aria-hidden={!open}>
        <div className="drawer-head">
          <span className="brand">
            <span className="logo-badge">📊</span> BrandScope
          </span>
          <button className="icon-btn" onClick={() => setOpen(false)} aria-label="Close">
            <X size={20} />
          </button>
        </div>

        <nav className="drawer-nav">
          {items.map((item) => {
            const Icon = item.icon;
            const active = location.pathname === item.to;
            return (
              <button
                key={item.to + item.label}
                className={`drawer-item ${active ? 'active' : ''}`}
                onClick={() => go(item.to)}
              >
                <Icon size={18} />
                {item.label}
              </button>
            );
          })}
          {profile && (
            <button className="drawer-item logout" onClick={logout}>
              <LogOut size={18} />
              {t('logout')}
            </button>
          )}
        </nav>

        <div className="drawer-foot">
          <p className="card-title" style={{ textAlign: 'center' }}>{t('settings.theme')}</p>
          <div className="row" style={{ gap: 6, justifyContent: 'center' }}>
            <button className={`mini-toggle ${theme === 'light' ? 'active' : ''}`} onClick={() => setTheme('light')} title="Light">
              <Sun size={15} /> {t('settings.light')}
            </button>
            <button className={`mini-toggle ${theme === 'dark' ? 'active' : ''}`} onClick={() => setTheme('dark')} title="Dark">
              <Moon size={15} /> {t('settings.dark')}
            </button>
          </div>
          <p className="card-title" style={{ textAlign: 'center', marginTop: 14 }}>{t('settings.title_lang')}</p>
          <div className="row" style={{ gap: 6, justifyContent: 'center' }}>
            {M.map((code) => (
              <button key={code} className={`mini-toggle lang ${lang === code ? 'active' : ''}`} onClick={() => setLang(code)}>
                {code === 'en' ? 'EN' : code === 'hi' ? 'हिं' : 'मरा'}
              </button>
            ))}
          </div>
        </div>
      </aside>
    </>
  );
}