import Link from "next/link";
import { notFound } from "next/navigation";
import { prisma } from "@/lib/db";
import { createGroup, createSet } from "@/app/actions";

export default async function GroupPage(props: PageProps<"/groups/[id]">) {
  const { id } = await props.params;

  const group = await prisma.group.findUnique({
    where: { id },
    include: {
      parent: true,
      children: { orderBy: { name: "asc" } },
      sets: { orderBy: { name: "asc" } },
    },
  });

  if (!group) notFound();

  return (
    <div className="space-y-8">
      {/* Breadcrumb */}
      <nav className="flex items-center gap-2 text-sm text-zinc-500 dark:text-zinc-400">
        <Link href="/" className="hover:text-zinc-900 dark:hover:text-zinc-100">Library</Link>
        {group.parent && (
          <>
            <span>/</span>
            <Link href={`/groups/${group.parent.id}`} className="hover:text-zinc-900 dark:hover:text-zinc-100">
              {group.parent.name}
            </Link>
          </>
        )}
        <span>/</span>
        <span className="text-zinc-900 dark:text-zinc-100 font-medium">{group.name}</span>
      </nav>

      <h1 className="text-2xl font-bold">{group.name}</h1>

      {/* Subgroups */}
      {group.children.length > 0 && (
        <section className="space-y-3">
          <h2 className="text-xs font-semibold uppercase tracking-wider text-zinc-500 dark:text-zinc-400">Groups</h2>
          <div className="grid gap-3">
            {group.children.map((child) => (
              <Link
                key={child.id}
                href={`/groups/${child.id}`}
                className="flex items-center justify-between rounded-lg border border-zinc-200 dark:border-zinc-800 bg-white dark:bg-zinc-900 px-5 py-4 hover:border-zinc-400 dark:hover:border-zinc-600 transition-colors"
              >
                <span className="font-medium">{child.name}</span>
                <span className="text-zinc-400 text-sm">→</span>
              </Link>
            ))}
          </div>
        </section>
      )}

      {/* Sets */}
      {group.sets.length > 0 && (
        <section className="space-y-3">
          <h2 className="text-xs font-semibold uppercase tracking-wider text-zinc-500 dark:text-zinc-400">Sets</h2>
          <div className="grid gap-3">
            {group.sets.map((set) => (
              <Link
                key={set.id}
                href={`/sets/${set.id}`}
                className="flex items-center justify-between rounded-lg border border-zinc-200 dark:border-zinc-800 bg-white dark:bg-zinc-900 px-5 py-4 hover:border-zinc-400 dark:hover:border-zinc-600 transition-colors"
              >
                <span className="font-medium">{set.name}</span>
                <span className="text-zinc-400 text-sm">→</span>
              </Link>
            ))}
          </div>
        </section>
      )}

      {group.children.length === 0 && group.sets.length === 0 && (
        <p className="text-zinc-400 dark:text-zinc-500 text-sm">Empty group. Add a subgroup or set below.</p>
      )}

      {/* Create subgroup */}
      <section className="space-y-3 border-t border-zinc-200 dark:border-zinc-800 pt-6">
        <h2 className="text-xs font-semibold uppercase tracking-wider text-zinc-500 dark:text-zinc-400">Add</h2>
        <form action={createGroup} className="flex gap-2">
          <input type="hidden" name="parentId" value={id} />
          <input
            name="name"
            required
            placeholder="New subgroup name"
            className="flex-1 rounded-lg border border-zinc-200 dark:border-zinc-800 bg-white dark:bg-zinc-900 px-4 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-zinc-400"
          />
          <button
            type="submit"
            className="rounded-lg border border-zinc-200 dark:border-zinc-800 bg-white dark:bg-zinc-900 px-4 py-2 text-sm font-medium hover:bg-zinc-100 dark:hover:bg-zinc-800 transition-colors"
          >
            + Group
          </button>
        </form>

        <form action={createSet} className="flex gap-2">
          <input type="hidden" name="groupId" value={id} />
          <input
            name="name"
            required
            placeholder="New set name"
            className="flex-1 rounded-lg border border-zinc-200 dark:border-zinc-800 bg-white dark:bg-zinc-900 px-4 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-zinc-400"
          />
          <button
            type="submit"
            className="rounded-lg bg-zinc-900 dark:bg-zinc-100 text-white dark:text-zinc-900 px-4 py-2 text-sm font-medium hover:bg-zinc-700 dark:hover:bg-zinc-300 transition-colors"
          >
            + Set
          </button>
        </form>
      </section>
    </div>
  );
}
