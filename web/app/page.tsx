import { prisma } from "@/lib/db";
import { createGroup } from "@/app/actions";
import { ItemRow } from "@/app/components/item-row";

export default async function Home() {
  const [groups, sets] = await Promise.all([
    prisma.group.findMany({ where: { parentId: null }, orderBy: { name: "asc" } }),
    prisma.set.findMany({ where: { groupId: undefined }, orderBy: { name: "asc" }, take: 0 }),
  ]);

  return (
    <div className="space-y-8">
      <div>
        <h1 className="text-2xl font-bold mb-1">Your Library</h1>
        <p className="text-zinc-500 dark:text-zinc-400 text-sm">Organize your flashcard sets into groups</p>
      </div>

      {groups.length === 0 ? (
        <p className="text-zinc-400 dark:text-zinc-500 text-sm">No groups yet. Create one below to get started.</p>
      ) : (
        <div className="grid gap-3">
          {groups.map((g) => (
            <ItemRow key={g.id} id={g.id} name={g.name} href={`/groups/${g.id}`} type="group" />
          ))}
        </div>
      )}

      <form action={createGroup} className="flex gap-2">
        <input type="hidden" name="parentId" value="" />
        <input
          name="name"
          required
          placeholder="New group name"
          className="flex-1 rounded-lg border border-zinc-200 dark:border-zinc-800 bg-white dark:bg-zinc-900 px-4 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-zinc-400"
        />
        <button
          type="submit"
          className="rounded-lg bg-zinc-900 dark:bg-zinc-100 text-white dark:text-zinc-900 px-4 py-2 text-sm font-medium hover:bg-zinc-700 dark:hover:bg-zinc-300 transition-colors"
        >
          + Group
        </button>
      </form>
    </div>
  );
}
