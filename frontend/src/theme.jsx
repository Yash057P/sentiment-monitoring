import { createContext, useContext, useEffect, useMemo, useState } from 'react';

const THEME_KEY = 'brandscope_theme';

export function getTheme() {
  return localStorage.getItem(THEME_KEY) || 'light';
}

export function applyTheme(theme) {
  document.documentElement.setAttribute('data-theme', theme);
}

const ThemeCtx = createContext({ theme: 'light', setTheme: () => {} });

export function ThemeProvider({ children }) {
  const [theme, setThemeState] = useState(getTheme);

  useEffect(() => {
    applyTheme(theme);
  }, [theme]);

  const setTheme = (next) => {
    localStorage.setItem(THEME_KEY, next);
    setThemeState(next);
  };

  const value = useMemo(() => ({ theme, setTheme }), [theme]);
  return <ThemeCtx.Provider value={value}>{children}</ThemeCtx.Provider>;
}

export function useTheme() {
  return useContext(ThemeCtx);
}

export function useChartTheme() {
  const { theme } = useTheme();
  return useMemo(
    () => ({
      tick: theme === 'dark' ? '#94a3b8' : '#64748b',
      grid: theme === 'dark' ? '#334155' : '#e2e8f0',
    }),
    [theme]
  );
}