import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import io

plt.switch_backend('Agg')

def create_temperature_trends_plot(daily_df):
    """Biểu đồ xu hướng nhiệt độ trung bình, min, max hàng ngày."""
    fig = plt.figure(figsize=(12, 6))
    plt.plot(daily_df.index, daily_df["Temp_mean"], color="orange", label="Trung bình")
    plt.plot(daily_df.index, daily_df["Temp_min"], color="skyblue", linestyle="--", label="Thấp nhất")
    plt.plot(daily_df.index, daily_df["Temp_max"], color="red", linestyle="--", label="Cao nhất")
    plt.fill_between(daily_df.index, daily_df["Temp_min"], daily_df["Temp_max"], color="peachpuff", alpha=0.3)
    plt.title("Biến động nhiệt độ theo ngày (từ file CSV)", fontsize=14)
    plt.xlabel("Ngày")
    plt.ylabel("Nhiệt độ (°C)")
    plt.legend()
    plt.grid(True, linestyle='--', alpha=0.6)
    plt.tight_layout()
    
    buf = io.BytesIO()
    fig.savefig(buf, format='png')
    buf.seek(0)
    plt.close(fig)
    return buf

def create_distributions_plot(hourly_df):
    """Biểu đồ phân bố tần suất của nhiệt độ và độ ẩm."""
    fig, axes = plt.subplots(1, 2, figsize=(12, 5))
    axes[0].hist(hourly_df["Temperature"], bins=25, color="orange", edgecolor="black", alpha=0.7)
    axes[0].set_title("Phân bố nhiệt độ", fontsize=13)
    axes[0].set_xlabel("Nhiệt độ (°C)")
    axes[0].set_ylabel("Tần suất")
    axes[1].hist(hourly_df["Humidity"], bins=25, color="skyblue", edgecolor="black", alpha=0.7)
    axes[1].set_title("Phân bố độ ẩm", fontsize=13)
    axes[1].set_xlabel("Độ ẩm (%)")
    plt.tight_layout()

    buf = io.BytesIO()
    fig.savefig(buf, format='png')
    buf.seek(0)
    plt.close(fig)
    return buf

def create_temp_humidity_scatter_plot(hourly_df):
    """Biểu đồ scatter plot thể hiện tương quan giữa nhiệt độ và độ ẩm."""
    fig = plt.figure(figsize=(6, 5))
    plt.scatter(hourly_df["Temperature"], hourly_df["Humidity"], alpha=0.4, color="teal", edgecolor="black", s=15)
    plt.title("Tương quan Nhiệt độ và Độ ẩm", fontsize=13)
    plt.xlabel("Nhiệt độ (°C)")
    plt.ylabel("Độ ẩm (%)")
    plt.grid(True, linestyle='--', alpha=0.6)
    plt.tight_layout()

    buf = io.BytesIO()
    fig.savefig(buf, format='png')
    buf.seek(0)
    plt.close(fig)
    return buf

def create_precip_temp_plot(daily_df):
    """Biểu đồ 2 trục thể hiện tương quan giữa Lượng mưa và Nhiệt độ."""
    fig, ax1 = plt.subplots(figsize=(12, 5))
    
    # Left axis: Precipitation (Bar chart)
    ax1.set_xlabel("Ngày")
    ax1.set_ylabel("Lượng mưa (mm)", color="royalblue")
    ax1.bar(daily_df.index, daily_df["Precip"], color="skyblue", label="Lượng mưa (mm)")
    ax1.tick_params(axis='y', labelcolor="royalblue")
    
    # Right axis: Temperature (Line chart)
    ax2 = ax1.twinx()
    ax2.plot(daily_df.index, daily_df["Temp_mean"], color="orange", label="Nhiệt độ TB (°C)")
    ax2.set_ylabel("Nhiệt độ (°C)", color="orange")
    ax2.tick_params(axis='y', labelcolor="orange")
    
    plt.title("Mối quan hệ Lượng mưa và Nhiệt độ trung bình", fontsize=14)
    fig.tight_layout()

    buf = io.BytesIO()
    fig.savefig(buf, format='png')
    buf.seek(0)
    plt.close(fig)
    return buf

def create_humidity_precip_plot(daily_df):
    """Biểu đồ 2 trục thể hiện tương quan giữa độ ẩm và lượng mưa."""
    fig, ax1 = plt.subplots(figsize=(12, 6))
    
    # Left axis: Humidity
    ax1.plot(daily_df.index, daily_df["Humidity"], color="royalblue", label="Độ ẩm trung bình (%)", linewidth=2)
    ax1.set_xlabel("Ngày")
    ax1.set_ylabel("Độ ẩm (%)", color="royalblue")
    ax1.tick_params(axis="y", labelcolor="royalblue")
    
    # Right axis: Precipitation
    ax2 = ax1.twinx()
    ax2.bar(daily_df.index, daily_df["Precip"], color="lightblue", alpha=0.6, label="Lượng mưa (mm)")
    ax2.set_ylabel("Lượng mưa (mm)", color="teal")
    ax2.tick_params(axis="y", labelcolor="teal")
    
    plt.title("Tương quan Độ ẩm và Lượng mưa theo ngày", fontsize=14)
    fig.legend(loc="upper right", bbox_to_anchor=(0.9, 0.9))
    plt.grid(True, linestyle='--', alpha=0.3)
    plt.tight_layout()

    buf = io.BytesIO()
    fig.savefig(buf, format='png')
    buf.seek(0)
    plt.close(fig)
    return buf

def create_correlation_heatmap(hourly_df):
    """Ma trận tương quan giữa các yếu tố thời tiết."""
    fig = plt.figure(figsize=(8, 6))
    cols_for_corr = ["Temperature", "Humidity", "Precipitation", "WindSpeed", "Pressure", "CloudCover"]
    # Ensure only existing columns are used to prevent errors
    existing_cols = [col for col in cols_for_corr if col in hourly_df.columns]
    corr = hourly_df[existing_cols].corr()
    
    sns.heatmap(corr, annot=True, cmap="coolwarm", fmt=".2f")
    plt.title("Ma trận tương quan (từ file CSV)", fontsize=14)
    plt.tight_layout()
    
    buf = io.BytesIO()
    fig.savefig(buf, format='png')
    buf.seek(0)
    plt.close(fig)
    return buf

