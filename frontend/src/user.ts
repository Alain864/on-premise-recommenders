import { createContext, useContext } from "react";

const STORAGE_KEY = "recommender.userId";

export function readStoredUserId(): string {
  return localStorage.getItem(STORAGE_KEY) ?? "";
}

export function storeUserId(userId: string) {
  if (userId) {
    localStorage.setItem(STORAGE_KEY, userId);
  } else {
    localStorage.removeItem(STORAGE_KEY);
  }
}

export const UserIdContext = createContext<{
  userId: string;
  setUserId: (value: string) => void;
}>({ userId: "", setUserId: () => undefined });

export function useUserId() {
  return useContext(UserIdContext);
}
