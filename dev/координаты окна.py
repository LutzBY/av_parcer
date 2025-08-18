import tkinter as tk

root = tk.Tk()
print("Размер экрана:", root.winfo_screenwidth(), "x", root.winfo_screenheight())
# Это — размер ОСНОВНОГО монитора

# Абсолютные координаты мыши (показывают, в какой зоне ты находишься)
def show_pos(event):
    print(f"Курсор: X={event.x_root}, Y={event.y_root}")

root.bind("<Motion>", show_pos)
root.attributes('-alpha', 0.1)  # прозрачность
try:
    root.mainloop()
except KeyboardInterrupt:
    root.destroy()